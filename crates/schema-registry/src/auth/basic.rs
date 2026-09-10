//! HTTP Basic credential store. It is the only new auth primitive, and the rest
//! reuses `krabka-security`. A plaintext credential gives cp
//! `PropertyFileLoginModule` parity. A `$2…` value is bcrypt-verified.
use std::collections::{HashMap, HashSet};

#[derive(Clone)]
struct BasicUser {
    credential: String,
    roles: Vec<String>,
}

impl BasicUser {
    fn parse(value: &str) -> Self {
        let mut fields = value.split(',');
        Self {
            credential: fields.next().unwrap_or_default().trim().to_owned(),
            roles: fields
                .map(str::trim)
                .filter(|role| !role.is_empty())
                .map(str::to_owned)
                .collect(),
        }
    }
}

/// Username → credential map for HTTP Basic auth. A stored value that begins
/// with `$2` is a bcrypt hash. Any other value is a plaintext password.
#[derive(Clone, Default)]
pub struct BasicAuthStore {
    users: HashMap<String, BasicUser>,
    required_roles: HashSet<String>,
}

impl std::fmt::Debug for BasicAuthStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BasicAuthStore")
    }
}

impl BasicAuthStore {
    /// Build directly from an in-memory `user -> credential` map.
    #[must_use]
    pub fn from_users(users: HashMap<String, String>) -> Self {
        Self {
            users: users
                .into_iter()
                .map(|(user, credential)| (user, BasicUser::parse(&credential)))
                .collect(),
            required_roles: HashSet::new(),
        }
    }

    /// Build from config. It reads the htpasswd-style `user:cred` file lines
    /// first, then layers the inline `users` map on top. The inline map wins.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`std::io::Error`] if `cfg.file` is set but cannot
    /// be read.
    pub fn load(cfg: &crate::config::BasicAuthConfig) -> std::io::Result<Self> {
        let mut users = HashMap::new();
        if let Some(path) = &cfg.file {
            for line in std::fs::read_to_string(path)?.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                if let Some((user, credential)) = line.split_once(':') {
                    let user = user.trim();
                    if !user.is_empty() {
                        users.insert(user.to_owned(), BasicUser::parse(credential));
                    }
                }
            }
        }
        users.extend(
            cfg.users
                .iter()
                .map(|(user, credential)| (user.trim().to_owned(), BasicUser::parse(credential)))
                .filter(|(user, _)| !user.is_empty()),
        );
        Ok(Self {
            users,
            required_roles: cfg.required_roles.clone(),
        })
    }

    /// Authenticate `user`/`pass` and return the user's configured roles.
    #[must_use]
    pub fn authenticate(&self, user: &str, pass: &str) -> Option<&[String]> {
        let stored = self.users.get(user)?;
        let credential_matches = if stored.credential.starts_with("$2") {
            bcrypt::verify(pass, &stored.credential).unwrap_or(false)
        } else {
            constant_time_eq(stored.credential.as_bytes(), pass.as_bytes())
        };
        let role_matches = self.required_roles.is_empty()
            || stored
                .roles
                .iter()
                .any(|role| self.required_roles.contains(role))
            || (self.required_roles.contains("*") && !stored.roles.is_empty());
        if !credential_matches || !role_matches {
            return None;
        }
        Some(&stored.roles)
    }

    /// Verify `user`/`pass` and any configured role requirement.
    #[must_use]
    pub fn verify(&self, user: &str, pass: &str) -> bool {
        self.authenticate(user, pass).is_some()
    }
}

/// Length-independent constant-time byte compare that needs no extra
/// dependency.
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    for (index, expected) in a.iter().enumerate() {
        diff |= usize::from(expected ^ b.get(index).copied().unwrap_or_default());
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plaintext_verify() {
        let s = BasicAuthStore::from_users(
            [("alice".to_string(), "pw".to_string())]
                .into_iter()
                .collect(),
        );
        for (user, pass, want) in [
            ("alice", "pw", true),
            ("alice", "bad", false),
            ("bob", "pw", false),
        ] {
            assert2::assert!(s.verify(user, pass) == want);
        }
    }

    #[test]
    fn bcrypt_verify() {
        let hash = bcrypt::hash("pw", 4).unwrap();
        let s = BasicAuthStore::from_users([("alice".to_string(), hash)].into_iter().collect());
        for (_name, password, expected) in [("matching", "pw", true), ("wrong", "bad", false)] {
            assert2::assert!(s.verify("alice", password) == expected);
        }
    }

    #[test]
    fn load_inline_users_win_over_file() {
        // htpasswd file says alice:filepw; inline config says alice:inlinepw.
        // Inline (CLI/config) is more explicit and must win the conflict.
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "krabka-sr-basic-auth-{}.htpasswd",
            std::process::id()
        ));
        std::fs::write(&path, "# comment\n\nalice:filepw\nbob:bobpw\n").unwrap();

        let cfg = crate::config::BasicAuthConfig {
            users: [("alice".to_string(), "inlinepw".to_string())]
                .into_iter()
                .collect(),
            file: Some(path.clone()),
            required_roles: HashSet::new(),
        };
        let store = BasicAuthStore::load(&cfg).unwrap();
        std::fs::remove_file(&path).ok();

        for (user, pass, want, _why) in [
            ("alice", "inlinepw", true, "inline credential wins"),
            ("alice", "filepw", false, "file credential is overridden"),
            // File-only entries (no inline override) still load.
            ("bob", "bobpw", true, "file-only entry preserved"),
        ] {
            assert2::assert!(store.verify(user, pass) == want);
        }
    }

    #[test]
    fn load_file_skips_comments_and_blank_lines() {
        // A file-ONLY load (no inline users): comments (`#`), blank lines, and a
        // colon-less malformed line are all skipped; valid `user:cred` lines load.
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "krabka-sr-basic-parse-{}.htpasswd",
            std::process::id()
        ));
        std::fs::write(
            &path,
            "# leading comment\n\n  \nalice:pw\nmalformed-no-colon\n  bob:bpw  \n",
        )
        .unwrap();

        let cfg = crate::config::BasicAuthConfig {
            users: HashMap::new(),
            file: Some(path.clone()),
            required_roles: HashSet::new(),
        };
        let store = BasicAuthStore::load(&cfg).unwrap();
        std::fs::remove_file(&path).ok();

        for (user, pass, want, _why) in [
            ("alice", "pw", true, "plain entry loads"),
            ("bob", "bpw", true, "leading/trailing ws trimmed"),
            // The colon-less line produced no entry, so nothing matches it.
            ("malformed-no-colon", "", false, "colon-less line skipped"),
        ] {
            assert2::assert!(store.verify(user, pass) == want);
        }
    }

    #[test]
    fn load_missing_file_is_io_err() {
        // `cfg.file` points at a path that does not exist → the underlying
        // io::Error propagates (NotFound).
        let cfg = crate::config::BasicAuthConfig {
            users: HashMap::new(),
            file: Some(std::path::PathBuf::from(
                "/nonexistent/krabka-sr-basic-missing.htpasswd",
            )),
            required_roles: HashSet::new(),
        };
        let err = BasicAuthStore::load(&cfg).expect_err("missing file must error");
        assert2::assert!(err.kind() == std::io::ErrorKind::NotFound);
    }

    #[test]
    fn cp_properties_roles_are_parsed_and_enforced() {
        let path = std::env::temp_dir().join(format!(
            "krabka-sr-basic-roles-{}.properties",
            std::process::id()
        ));
        std::fs::write(&path, " alice : pw , admin , developer\nbob: other,user\n").unwrap();
        let cfg = crate::config::BasicAuthConfig {
            users: HashMap::new(),
            file: Some(path.clone()),
            required_roles: ["admin".to_owned()].into_iter().collect(),
        };
        let store = BasicAuthStore::load(&cfg).unwrap();
        let wildcard_store = BasicAuthStore::load(&crate::config::BasicAuthConfig {
            required_roles: ["*".to_owned()].into_iter().collect(),
            ..cfg.clone()
        })
        .unwrap();
        std::fs::remove_file(path).ok();

        for (user, password, expected) in [
            ("alice", "pw", true),
            ("alice", "pw,admin", false),
            ("bob", "other", false),
        ] {
            assert2::assert!(store.verify(user, password) == expected);
        }
        assert2::assert!(
            store.authenticate("alice", "pw").unwrap()
                == ["admin".to_owned(), "developer".to_owned()]
        );
        assert2::assert!(wildcard_store.verify("bob", "other"));
        assert2::assert!(format!("{store:?}") == "BasicAuthStore");
    }
}
