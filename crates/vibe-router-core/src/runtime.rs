//! Hardware-aware adaptive runtime profiler and portable drive abstraction.
//!
//! Provides:
//!   * Runtime profiles: `Auto`, `Vps` (Disk-First, Ultra-Low RAM),
//!     `Portable` (RAM-First, Zero Flash Wear), and `Workstation`.
//!   * Automatic detection of host RAM, CPU cores, and USB portable drive markers.
//!   * Drive-letter independent path expansion and normalization (`{{PORTABLE_ROOT}}`).

use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Target operating profile for Vibe Router.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeProfile {
    #[default]
    Auto,
    Vps,
    Portable,
    Workstation,
}

impl FromStr for RuntimeProfile {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(RuntimeProfile::Auto),
            "vps" | "low-ram" | "disk-first" => Ok(RuntimeProfile::Vps),
            "portable" | "ram-first" | "usb" => Ok(RuntimeProfile::Portable),
            "workstation" | "desktop" | "server" => Ok(RuntimeProfile::Workstation),
            other => Err(format!("unknown runtime profile: `{other}`")),
        }
    }
}

impl std::fmt::Display for RuntimeProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeProfile::Auto => write!(f, "auto"),
            RuntimeProfile::Vps => write!(f, "vps"),
            RuntimeProfile::Portable => write!(f, "portable"),
            RuntimeProfile::Workstation => write!(f, "workstation"),
        }
    }
}

/// Resolved hardware capabilities and runtime parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardwareProfile {
    /// Active effective profile.
    pub profile: RuntimeProfile,
    /// Total host physical RAM in Megabytes.
    pub total_ram_mb: u64,
    /// Available logical CPU cores.
    pub cpu_cores: usize,
    /// True if running from a portable environment (`.portable` marker or `portable_data/`).
    pub is_portable: bool,
    /// Absolute path to the root of the portable drive/directory if detected.
    pub portable_root: Option<String>,
    /// Effective cache capacity in MB for `vibe-router-cache`.
    pub cache_capacity_mb: usize,
    /// If true, use in-memory SQLite with periodic flush (zero flash wear on USB).
    pub memory_first_storage: bool,
}

impl HardwareProfile {
    /// Detect hardware resources and resolve the active execution profile.
    pub fn detect(profile_override: Option<RuntimeProfile>) -> Self {
        let total_ram_mb = detect_ram_mb();
        let cpu_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let (is_portable, portable_root) = detect_portable_environment();

        let explicit_profile = profile_override
            .or_else(|| {
                std::env::var("VIBE_ROUTER_PROFILE")
                    .ok()
                    .and_then(|v| v.parse().ok())
            })
            .unwrap_or(RuntimeProfile::Auto);

        let effective_profile = match explicit_profile {
            RuntimeProfile::Vps => RuntimeProfile::Vps,
            RuntimeProfile::Portable => RuntimeProfile::Portable,
            RuntimeProfile::Workstation => RuntimeProfile::Workstation,
            RuntimeProfile::Auto => {
                if is_portable {
                    if total_ram_mb >= 4096 {
                        RuntimeProfile::Portable
                    } else {
                        RuntimeProfile::Vps
                    }
                } else if total_ram_mb < 1024 {
                    RuntimeProfile::Vps
                } else {
                    RuntimeProfile::Workstation
                }
            }
        };

        let (cache_capacity_mb, memory_first_storage) = match effective_profile {
            RuntimeProfile::Vps => (4, false),       // 4MB cache, low RSS
            RuntimeProfile::Portable => (256, true), // 256MB cache, RAM-first zero wear
            RuntimeProfile::Workstation | RuntimeProfile::Auto => (256, false),
        };

        Self {
            profile: effective_profile,
            total_ram_mb,
            cpu_cores,
            is_portable,
            portable_root,
            cache_capacity_mb,
            memory_first_storage,
        }
    }

    /// Expand any `{{PORTABLE_ROOT}}` placeholder into the concrete runtime path.
    pub fn expand_path(&self, raw: &str) -> String {
        expand_portable_path(raw, self.portable_root.as_deref())
    }

    /// Normalize an absolute path on a portable drive to use `{{PORTABLE_ROOT}}`.
    pub fn normalize_path(&self, path: &str) -> String {
        normalize_to_portable(path, self.portable_root.as_deref())
    }
}

/// Detect total system RAM in Megabytes without unsafe code.
pub fn detect_ram_mb() -> u64 {
    if let Ok(v) = std::env::var("VIBE_ROUTER_RAM_MB") {
        if let Ok(mb) = v.parse::<u64>() {
            return mb;
        }
    }

    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
            for line in content.lines() {
                if line.starts_with("MemTotal:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let Ok(kb) = parts[1].parse::<u64>() {
                            return kb / 1024;
                        }
                    }
                }
            }
        }
    }

    // Default: 8192 MB (8GB) for desktop/workstation environments
    8192
}

/// Detect if the binary is running in a portable installation.
pub fn detect_portable_environment() -> (bool, Option<String>) {
    // Check current working directory first
    if Path::new(".portable").exists() || Path::new("portable_data").is_dir() {
        if let Ok(cwd) = std::env::current_dir() {
            return (true, Some(clean_path_str(&cwd)));
        }
    }

    // Check directory of executable
    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(parent) = exe_path.parent() {
            if parent.join(".portable").exists() || parent.join("portable_data").is_dir() {
                return (true, Some(clean_path_str(parent)));
            }
        }
    }

    (false, None)
}

fn clean_path_str(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// Replace `{{PORTABLE_ROOT}}` in `path` with the actual portable root.
pub fn expand_portable_path(path: &str, portable_root: Option<&str>) -> String {
    if !path.contains("{{PORTABLE_ROOT}}") {
        return path.to_string();
    }
    let root = portable_root.unwrap_or(".");
    path.replace("{{PORTABLE_ROOT}}", root)
}

/// Replace the concrete portable root prefix with `{{PORTABLE_ROOT}}`.
pub fn normalize_to_portable(path: &str, portable_root: Option<&str>) -> String {
    let Some(root) = portable_root else {
        return path.to_string();
    };
    let normalized_path = path.replace('\\', "/");
    let normalized_root = root.replace('\\', "/");

    if normalized_path.starts_with(&normalized_root) {
        let suffix = &normalized_path[normalized_root.len()..];
        let suffix = suffix.strip_prefix('/').unwrap_or(suffix);
        format!("{{{{PORTABLE_ROOT}}}}/{suffix}")
    } else {
        path.to_string()
    }
}

/// Setup portable temp directory inside `portable_data/tmp` to maintain zero host footprint.
pub fn setup_portable_temp(portable_root: Option<&str>) -> Option<PathBuf> {
    let root = portable_root?;
    let tmp = Path::new(root).join("portable_data").join("tmp");
    if std::fs::create_dir_all(&tmp).is_ok() {
        std::env::set_var("TMP", &tmp);
        std::env::set_var("TEMP", &tmp);
        Some(tmp)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hardware_profiler_detection() {
        let hp = HardwareProfile::detect(Some(RuntimeProfile::Workstation));
        assert_eq!(hp.profile, RuntimeProfile::Workstation);
        assert!(hp.total_ram_mb > 0);
        assert!(hp.cpu_cores > 0);
        assert_eq!(hp.cache_capacity_mb, 256);
        assert!(!hp.memory_first_storage);
    }

    #[test]
    fn test_vps_profile_tunables() {
        let hp = HardwareProfile::detect(Some(RuntimeProfile::Vps));
        assert_eq!(hp.profile, RuntimeProfile::Vps);
        assert_eq!(hp.cache_capacity_mb, 4);
        assert!(!hp.memory_first_storage);
    }

    #[test]
    fn test_portable_profile_tunables() {
        let hp = HardwareProfile::detect(Some(RuntimeProfile::Portable));
        assert_eq!(hp.profile, RuntimeProfile::Portable);
        assert_eq!(hp.cache_capacity_mb, 256);
        assert!(hp.memory_first_storage);
    }

    #[test]
    fn test_expand_portable_path() {
        let raw = "{{PORTABLE_ROOT}}/data/vibe-router.db";
        let expanded = expand_portable_path(raw, Some("E:/VibeRouter"));
        assert_eq!(expanded, "E:/VibeRouter/data/vibe-router.db");

        let no_placeholder = "C:/local/data.db";
        assert_eq!(
            expand_portable_path(no_placeholder, Some("E:/")),
            no_placeholder
        );
    }

    #[test]
    fn test_normalize_to_portable() {
        let abs = "E:/VibeRouter/data/vibe-router.db";
        let norm = normalize_to_portable(abs, Some("E:/VibeRouter"));
        assert_eq!(norm, "{{PORTABLE_ROOT}}/data/vibe-router.db");

        let win_abs = "E:\\VibeRouter\\config\\vibe.toml";
        let norm2 = normalize_to_portable(win_abs, Some("E:/VibeRouter"));
        assert_eq!(norm2, "{{PORTABLE_ROOT}}/config/vibe.toml");
    }
}
