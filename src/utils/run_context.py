import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4


# ============================================================
# Modèle du contexte d'exécution
# ============================================================

@dataclass(frozen=True)
class ProcessingRunContext:
    """
    Contexte technique associé à une exécution logique
    du pipeline.

    processing_run_id :
        identifiant unique de l'exécution.

    processing_started_at_utc :
        date/heure UTC de création du contexte.

    git_commit_sha :
        commit Git utilisé comme référence du code.

    git_worktree_dirty :
        True si le dépôt contient des modifications
        non commitées ou des fichiers non suivis.

    git_branch :
        branche Git active au moment de l'exécution.
    """

    processing_run_id: str
    processing_started_at_utc: str
    git_commit_sha: str
    git_worktree_dirty: bool
    git_branch: str

    def to_r2_metadata(
        self,
    ) -> dict[str, str]:
        """
        Convertit le contexte en métadonnées compatibles
        avec S3 / Cloudflare R2.

        Toutes les valeurs sont volontairement converties
        en chaînes de caractères.
        """

        return {
            "processing-run-id": (
                self.processing_run_id
            ),
            "processing-started-at-utc": (
                self.processing_started_at_utc
            ),
            "git-commit-sha": (
                self.git_commit_sha
            ),
            "git-worktree-dirty": (
                str(
                    self.git_worktree_dirty
                ).lower()
            ),
            "git-branch": (
                self.git_branch
            ),
        }


# ============================================================
# processing_run_id
# ============================================================

def normalize_processing_run_id(
    processing_run_id: UUID | str,
) -> str:
    """
    Valide et normalise un processing_run_id.

    La représentation retournée est toujours
    l'UUID canonique.
    """

    try:

        parsed_uuid = UUID(
            str(
                processing_run_id
            )
        )

    except (
        ValueError,
        TypeError,
        AttributeError,
    ) as exc:

        raise ValueError(
            "processing_run_id invalide : "
            f"{processing_run_id}"
        ) from exc

    return str(
        parsed_uuid
    )


def resolve_processing_run_id(
    processing_run_id: UUID | str | None = None,
) -> str:
    """
    Retourne le processing_run_id à utiliser.

    Deux cas :

    1. Un ID est fourni par l'orchestrateur :
       il est validé puis conservé.

    2. Aucun ID n'est fourni :
       un nouvel UUID est créé.

    Cette conception permet aujourd'hui une exécution CLI
    et demain une propagation depuis Airflow.
    """

    if processing_run_id is None:

        return str(
            uuid4()
        )

    return normalize_processing_run_id(
        processing_run_id
    )


# ============================================================
# Temps UTC
# ============================================================

def utc_now_iso() -> str:
    """
    Retourne le timestamp UTC courant au format ISO 8601.

    Exemple :
        2026-10-05T21:45:12Z
    """

    return (
        datetime.now(
            timezone.utc
        )
        .isoformat(
            timespec="seconds"
        )
        .replace(
            "+00:00",
            "Z",
        )
    )


# ============================================================
# Git
# ============================================================

def _run_git_command(
    arguments: list[str],
    repo_dir: Path | str | None = None,
) -> str:
    """
    Exécute une commande Git dans le dépôt courant
    et retourne stdout nettoyé.

    Une erreur Git provoque volontairement une exception :
    la provenance du code ne doit pas être inventée.
    """

    working_directory = (
        Path(repo_dir).resolve()
        if repo_dir is not None
        else Path.cwd().resolve()
    )

    try:

        result = subprocess.run(
            [
                "git",
                *arguments,
            ],
            cwd=working_directory,
            check=True,
            capture_output=True,
            text=True,
        )

    except FileNotFoundError as exc:

        raise RuntimeError(
            "Git n'est pas disponible "
            "dans l'environnement."
        ) from exc

    except subprocess.CalledProcessError as exc:

        stderr = (
            exc.stderr.strip()
            if exc.stderr
            else ""
        )

        raise RuntimeError(
            "Impossible de récupérer "
            "le contexte Git."
            + (
                f" Détail : {stderr}"
                if stderr
                else ""
            )
        ) from exc

    return (
        result.stdout.strip()
    )


def get_git_commit_sha(
    repo_dir: Path | str | None = None,
) -> str:
    """
    Retourne le SHA complet du commit HEAD.
    """

    commit_sha = (
        _run_git_command(
            [
                "rev-parse",
                "HEAD",
            ],
            repo_dir=repo_dir,
        )
    )

    if not commit_sha:
        raise RuntimeError(
            "SHA Git vide."
        )

    return commit_sha


def get_git_branch(
    repo_dir: Path | str | None = None,
) -> str:
    """
    Retourne la branche Git active.

    En mode detached HEAD, Git retourne 'HEAD'.
    """

    branch = (
        _run_git_command(
            [
                "rev-parse",
                "--abbrev-ref",
                "HEAD",
            ],
            repo_dir=repo_dir,
        )
    )

    if not branch:
        raise RuntimeError(
            "Branche Git indéterminée."
        )

    return branch


def is_git_worktree_dirty(
    repo_dir: Path | str | None = None,
) -> bool:
    """
    Retourne True si le dépôt contient :

    - des fichiers modifiés ;
    - des fichiers ajoutés/supprimés ;
    - des fichiers non suivis.

    C'est essentiel car un commit SHA seul ne suffit pas
    à garantir la reproductibilité si le code exécuté
    contient des changements non commités.
    """

    status = (
        _run_git_command(
            [
                "status",
                "--porcelain",
                "--untracked-files=normal",
            ],
            repo_dir=repo_dir,
        )
    )

    return bool(
        status
    )


# ============================================================
# Construction du contexte complet
# ============================================================

def build_processing_run_context(
    processing_run_id: UUID | str | None = None,
    repo_dir: Path | str | None = None,
) -> ProcessingRunContext:
    """
    Construit le contexte technique complet
    d'une exécution du pipeline.

    En CLI :
        processing_run_id peut être absent et sera créé.

    Avec Airflow :
        processing_run_id sera fourni explicitement
        puis propagé aux différentes tâches.
    """

    resolved_processing_run_id = (
        resolve_processing_run_id(
            processing_run_id
        )
    )

    processing_started_at_utc = (
        utc_now_iso()
    )

    git_commit_sha = (
        get_git_commit_sha(
            repo_dir=repo_dir
        )
    )

    git_worktree_dirty = (
        is_git_worktree_dirty(
            repo_dir=repo_dir
        )
    )

    git_branch = (
        get_git_branch(
            repo_dir=repo_dir
        )
    )

    return ProcessingRunContext(
        processing_run_id=(
            resolved_processing_run_id
        ),
        processing_started_at_utc=(
            processing_started_at_utc
        ),
        git_commit_sha=(
            git_commit_sha
        ),
        git_worktree_dirty=(
            git_worktree_dirty
        ),
        git_branch=(
            git_branch
        ),
    )
