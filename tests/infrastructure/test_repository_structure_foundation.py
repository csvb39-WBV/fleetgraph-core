from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

REQUIRED_DIRECTORIES = [
    "src/fleetgraph_core",
    "src/fleetgraph_core/api",
    "src/fleetgraph_core/commercialization",
    "src/fleetgraph_core/config",
    "src/fleetgraph_core/domain",
    "src/fleetgraph_core/infrastructure",
    "src/fleetgraph_core/infrastructure/aws",
    "src/fleetgraph_core/infrastructure/messaging",
    "src/fleetgraph_core/infrastructure/observability",
    "src/fleetgraph_core/infrastructure/scheduling",
    "src/fleetgraph_core/infrastructure/storage",
    "src/fleetgraph_core/intelligence",
    "src/fleetgraph_core/intelligence/collection",
    "src/fleetgraph_core/intelligence/feedback",
    "src/fleetgraph_core/intelligence/fusion",
    "src/fleetgraph_core/intelligence/predictive",
    "src/fleetgraph_core/intelligence/snapshot",
    "src/fleetgraph_core/intelligence/translation",
    "src/fleetgraph_core/security",
    "src/fleetgraph_core/ui_contracts",
    "tests",
    "tests/api",
    "tests/commercialization",
    "tests/infrastructure",
    "tests/intelligence",
    "tests/integration",
    "infra",
    "infra/ecs",
    "infra/iam",
    "infra/networking",
    "infra/terraform",
    "docs",
    "docs/architecture",
    "docs/contracts",
    "docs/operations",
    "frontend",
    "frontend/fleetgraph-ui",
]

REQUIRED_PACKAGE_DIRECTORIES = [
    "src/fleetgraph_core",
    "src/fleetgraph_core/api",
    "src/fleetgraph_core/commercialization",
    "src/fleetgraph_core/config",
    "src/fleetgraph_core/domain",
    "src/fleetgraph_core/infrastructure",
    "src/fleetgraph_core/infrastructure/aws",
    "src/fleetgraph_core/infrastructure/messaging",
    "src/fleetgraph_core/infrastructure/observability",
    "src/fleetgraph_core/infrastructure/scheduling",
    "src/fleetgraph_core/infrastructure/storage",
    "src/fleetgraph_core/intelligence",
    "src/fleetgraph_core/intelligence/collection",
    "src/fleetgraph_core/intelligence/feedback",
    "src/fleetgraph_core/intelligence/fusion",
    "src/fleetgraph_core/intelligence/predictive",
    "src/fleetgraph_core/intelligence/snapshot",
    "src/fleetgraph_core/intelligence/translation",
    "src/fleetgraph_core/security",
    "src/fleetgraph_core/ui_contracts",
]

REQUIRED_INTELLIGENCE_SUBPACKAGES = [
    "collection",
    "feedback",
    "fusion",
    "predictive",
    "snapshot",
    "translation",
]

REQUIRED_INFRASTRUCTURE_SUBPACKAGES = [
    "aws",
    "messaging",
    "observability",
    "scheduling",
    "storage",
]

ALLOWED_AWS_PACKAGE_ROOT = REPO_ROOT / "src/fleetgraph_core/infrastructure/aws"


def _structure_validation_snapshot() -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    missing_directories = tuple(
        sorted(
            directory
            for directory in REQUIRED_DIRECTORIES
            if not (REPO_ROOT / directory).is_dir()
        )
    )
    missing_package_inits = tuple(
        sorted(
            package_dir
            for package_dir in REQUIRED_PACKAGE_DIRECTORIES
            if not (REPO_ROOT / package_dir / "__init__.py").is_file()
        )
    )

    aws_directories_outside_boundary = []
    for directory in (REPO_ROOT / "src").rglob("*"):
        if not directory.is_dir() or directory.name.lower() != "aws":
            continue
        resolved_dir = directory.resolve()
        if resolved_dir == ALLOWED_AWS_PACKAGE_ROOT.resolve():
            continue
        if ALLOWED_AWS_PACKAGE_ROOT.resolve() in resolved_dir.parents:
            continue
        aws_directories_outside_boundary.append(str(directory.relative_to(REPO_ROOT)))

    return (
        missing_directories,
        missing_package_inits,
        tuple(sorted(aws_directories_outside_boundary)),
    )


def test_required_directories_exist() -> None:
    missing_directories, _, _ = _structure_validation_snapshot()
    assert not missing_directories, (
        "Missing required directories: " + ", ".join(missing_directories)
    )


def test_required_package_init_files_exist() -> None:
    _, missing_package_inits, _ = _structure_validation_snapshot()
    assert not missing_package_inits, (
        "Missing __init__.py in required package directories: "
        + ", ".join(missing_package_inits)
    )


def test_required_non_python_directories_exist() -> None:
    required_non_python_directories = [
        "infra",
        "infra/ecs",
        "infra/iam",
        "infra/networking",
        "infra/terraform",
        "docs",
        "docs/architecture",
        "docs/contracts",
        "docs/operations",
        "frontend",
        "frontend/fleetgraph-ui",
    ]
    missing_directories = [
        directory
        for directory in required_non_python_directories
        if not (REPO_ROOT / directory).is_dir()
    ]
    assert not missing_directories, (
        "Missing required non-Python directories: " + ", ".join(missing_directories)
    )


def test_intelligence_subpackages_exist() -> None:
    intelligence_root = REPO_ROOT / "src/fleetgraph_core/intelligence"
    missing_subpackages = [
        subpackage
        for subpackage in REQUIRED_INTELLIGENCE_SUBPACKAGES
        if not (intelligence_root / subpackage).is_dir()
    ]
    assert not missing_subpackages, (
        "Missing intelligence subpackages: " + ", ".join(missing_subpackages)
    )


def test_infrastructure_subpackages_exist() -> None:
    infrastructure_root = REPO_ROOT / "src/fleetgraph_core/infrastructure"
    missing_subpackages = [
        subpackage
        for subpackage in REQUIRED_INFRASTRUCTURE_SUBPACKAGES
        if not (infrastructure_root / subpackage).is_dir()
    ]
    assert not missing_subpackages, (
        "Missing infrastructure subpackages: " + ", ".join(missing_subpackages)
    )


def test_aws_package_boundary_enforced() -> None:
    _, _, aws_directories_outside_boundary = _structure_validation_snapshot()
    assert not aws_directories_outside_boundary, (
        "AWS directories found outside src/fleetgraph_core/infrastructure/: "
        + ", ".join(aws_directories_outside_boundary)
    )


def test_structure_validation_is_stable_across_repeated_runs() -> None:
    first_snapshot = _structure_validation_snapshot()
    second_snapshot = _structure_validation_snapshot()
    assert first_snapshot == second_snapshot