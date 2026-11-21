package setup

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// dockerOneLiner starts a ready-to-use PostgreSQL with pgvector and logical
// replication enabled (the fastest route when Docker is installed).
const dockerOneLiner = `docker run -d --name embedsync-postgres -p 5432:5432 -e POSTGRES_PASSWORD=postgres \
    -v embedsync-pgdata:/var/lib/postgresql pgvector/pgvector:pg17 -c wal_level=logical`

// postgresInstalled reports whether PostgreSQL client or server binaries are present.
func postgresInstalled() bool {
	for _, b := range []string{"psql", "postgres", "pg_ctl"} {
		if _, err := exec.LookPath(b); err == nil {
			return true
		}
	}
	var globs []string
	switch runtime.GOOS {
	case "windows":
		globs = []string{`C:\Program Files\PostgreSQL\*\bin\psql.exe`}
	case "darwin":
		globs = []string{"/opt/homebrew/opt/postgresql@*/bin/psql", "/usr/local/opt/postgresql@*/bin/psql",
			"/Applications/Postgres.app/Contents/Versions/*/bin/psql"}
	default:
		globs = []string{"/usr/lib/postgresql/*/bin/postgres", "/usr/pgsql-*/bin/postgres"}
	}
	for _, g := range globs {
		if m, _ := filepath.Glob(g); len(m) > 0 {
			return true
		}
	}
	return false
}

func linuxFamily() string {
	if _, err := os.Stat("/etc/debian_version"); err == nil {
		return "debian"
	}
	if _, err := os.Stat("/etc/redhat-release"); err == nil {
		return "redhat"
	}
	if b, err := os.ReadFile("/etc/os-release"); err == nil && strings.Contains(string(b), "fedora") {
		return "redhat"
	}
	return ""
}

// installPostgresHelp explains how to install PostgreSQL with pgvector on this OS.
func installPostgresHelp() string {
	var b strings.Builder
	b.WriteString("PostgreSQL (16, 17 or 18) with the pgvector extension is required. Install it:\n\n")
	switch runtime.GOOS {
	case "darwin":
		b.WriteString("  brew install postgresql@17 pgvector\n  brew services start postgresql@17\n\n" +
			"  or install Postgres.app (https://postgresapp.com), which includes pgvector.\n")
	case "windows":
		b.WriteString("  1. Download and run the installer: https://www.postgresql.org/download/windows/\n" +
			"     (remember the password you choose for the 'postgres' user)\n" +
			"  2. Install pgvector: https://github.com/pgvector/pgvector#windows\n")
	default:
		switch linuxFamily() {
		case "debian":
			b.WriteString("  sudo apt install -y postgresql\n" +
				"  sudo apt install -y \"postgresql-$(psql -V | grep -oE '[0-9]+' | head -1)-pgvector\"\n")
		case "redhat":
			b.WriteString("  sudo dnf install -y postgresql-server pgvector\n" +
				"  sudo postgresql-setup --initdb && sudo systemctl enable --now postgresql\n")
		default:
			b.WriteString("  https://www.postgresql.org/download/linux/ and https://github.com/pgvector/pgvector#installation\n")
		}
	}
	b.WriteString("\nOr, with Docker, one command gives a ready database (user postgres, password postgres):\n\n  " +
		dockerOneLiner + "\n\nThen run `embedsync setup` again.")
	return b.String()
}

// startPostgresHelp explains how to start an installed but stopped server.
func startPostgresHelp() string {
	switch runtime.GOOS {
	case "darwin":
		return "PostgreSQL is installed but not accepting connections. Start it, e.g.:\n\n" +
			"  brew services start postgresql@17      (Homebrew)\n  or open Postgres.app\n"
	case "windows":
		return "PostgreSQL is installed but not accepting connections. Start its service, e.g. in PowerShell as administrator:\n\n" +
			"  Get-Service postgresql* | Start-Service\n"
	default:
		return "PostgreSQL is installed but not accepting connections. Start it, e.g.:\n\n  sudo systemctl start postgresql\n"
	}
}

// pgvectorHelp explains how to add pgvector to an existing server.
func pgvectorHelp(major string) string {
	switch runtime.GOOS {
	case "darwin":
		return "  brew install pgvector        (Homebrew PostgreSQL; Postgres.app already includes it)\n"
	case "windows":
		return "  follow https://github.com/pgvector/pgvector#windows\n"
	default:
		switch linuxFamily() {
		case "debian":
			return "  sudo apt install -y postgresql-" + major + "-pgvector\n"
		case "redhat":
			return "  sudo dnf install -y pgvector\n"
		}
		return "  see https://github.com/pgvector/pgvector#installation\n"
	}
}

// restartHelp explains how to restart PostgreSQL after ALTER SYSTEM.
func restartHelp(major string) string {
	switch runtime.GOOS {
	case "darwin":
		return "  brew services restart postgresql@" + major + "   (Homebrew)\n  or stop and start the server in Postgres.app\n"
	case "windows":
		return "  Restart-Service postgresql-x64-" + major + "   (PowerShell as administrator)\n"
	default:
		return "  sudo systemctl restart postgresql\n  (Docker: docker restart <container>)\n"
	}
}
