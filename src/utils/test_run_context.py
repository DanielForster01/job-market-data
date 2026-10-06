import re
from datetime import datetime
from uuid import UUID

from src.utils.run_context import (
    build_processing_run_context,
    normalize_processing_run_id,
    resolve_processing_run_id,
)


# ============================================================
# Données de test
# ============================================================

FIXED_PROCESSING_RUN_ID = (
    "11111111-2222-4333-8444-555555555555"
)


# ============================================================
# Test ID fourni
# ============================================================

def test_fixed_processing_run_id() -> None:
    """
    Vérifie qu'un processing_run_id fourni
    est conservé après validation.
    """

    actual = (
        normalize_processing_run_id(
            FIXED_PROCESSING_RUN_ID
        )
    )

    assert (
        actual
        == FIXED_PROCESSING_RUN_ID
    )

    resolved = (
        resolve_processing_run_id(
            FIXED_PROCESSING_RUN_ID
        )
    )

    assert (
        resolved
        == FIXED_PROCESSING_RUN_ID
    )

    print(
        "[OK] processing_run_id fourni"
    )


# ============================================================
# Test génération automatique
# ============================================================

def test_generated_processing_run_id() -> None:
    """
    Vérifie qu'un UUID valide est créé
    lorsqu'aucun ID n'est fourni.
    """

    generated = (
        resolve_processing_run_id()
    )

    parsed = UUID(
        generated
    )

    assert (
        str(parsed)
        == generated
    )

    print(
        "[OK] Génération processing_run_id"
    )


# ============================================================
# Test ID invalide
# ============================================================

def test_invalid_processing_run_id() -> None:
    """
    Vérifie le rejet d'un identifiant invalide.
    """

    try:

        normalize_processing_run_id(
            "invalid-id"
        )

    except ValueError:

        print(
            "[OK] Rejet processing_run_id invalide"
        )

        return

    raise AssertionError(
        "Un processing_run_id invalide "
        "aurait dû provoquer ValueError."
    )


# ============================================================
# Test contexte complet
# ============================================================

def test_processing_context() -> None:
    """
    Vérifie le contexte d'exécution complet,
    notamment la provenance Git.
    """

    context = (
        build_processing_run_context(
            processing_run_id=(
                FIXED_PROCESSING_RUN_ID
            )
        )
    )

    assert (
        context.processing_run_id
        == FIXED_PROCESSING_RUN_ID
    )

    # --------------------------------------------------------
    # Timestamp UTC
    # --------------------------------------------------------

    assert (
        context.processing_started_at_utc
        .endswith(
            "Z"
        )
    )

    datetime.fromisoformat(
        context.processing_started_at_utc
        .replace(
            "Z",
            "+00:00",
        )
    )

    # --------------------------------------------------------
    # Git SHA
    # --------------------------------------------------------
    #
    # Compatible avec les dépôts Git SHA-1 classiques
    # et les dépôts utilisant SHA-256.
    #
    # --------------------------------------------------------

    assert re.fullmatch(
        r"[0-9a-f]{40}|[0-9a-f]{64}",
        context.git_commit_sha,
    )

    # --------------------------------------------------------
    # Branche
    # --------------------------------------------------------

    assert isinstance(
        context.git_branch,
        str,
    )

    assert (
        context.git_branch.strip()
        != ""
    )

    # --------------------------------------------------------
    # Dirty flag
    # --------------------------------------------------------

    assert isinstance(
        context.git_worktree_dirty,
        bool,
    )

    # --------------------------------------------------------
    # Conversion métadonnées R2
    # --------------------------------------------------------

    metadata = (
        context.to_r2_metadata()
    )

    assert (
        metadata[
            "processing-run-id"
        ]
        == FIXED_PROCESSING_RUN_ID
    )

    assert (
        metadata[
            "git-commit-sha"
        ]
        == context.git_commit_sha
    )

    assert (
        metadata[
            "git-worktree-dirty"
        ]
        in {
            "true",
            "false",
        }
    )

    assert (
        metadata[
            "git-branch"
        ]
        == context.git_branch
    )

    print(
        "[OK] Contexte processing complet"
    )

    print(
        f"     Git branch : "
        f"{context.git_branch}"
    )

    print(
        f"     Git commit : "
        f"{context.git_commit_sha}"
    )

    print(
        f"     Worktree dirty : "
        f"{context.git_worktree_dirty}"
    )


# ============================================================
# Exécution
# ============================================================

def main() -> None:

    print()
    print("=" * 70)
    print(
        "TEST DU CONTEXTE D'EXÉCUTION"
    )
    print("=" * 70)
    print()

    test_fixed_processing_run_id()

    test_generated_processing_run_id()

    test_invalid_processing_run_id()

    test_processing_context()

    print()
    print("-" * 70)
    print(
        "CONTEXTE D'EXÉCUTION : VALIDE"
    )
    print("-" * 70)
    print()


if __name__ == "__main__":
    main()
