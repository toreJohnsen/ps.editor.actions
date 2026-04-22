"""Generate multiple PlantUML diagrams grouped by package attribute."""
import json
from pathlib import Path
from collections import defaultdict
from typing import Any, Mapping, Sequence

from puml.feature_types import render_feature_types_to_puml

def generate_puml_by_package(
    feature_catalogue_path: Path,
    output_dir: Path,
    include_notes: bool = True,
    include_descriptions: bool = True,
    include_generalization: bool = True,
) -> None:
    """
    Generate multiple PlantUML diagrams, one per package.
    
    Args:
        feature_catalogue_path: Path to the feature_catalogue.json file
        output_dir: Directory to write the generated .puml files
        include_notes: Whether to include feature type notes
        include_descriptions: Whether to include attribute descriptions
        include_generalization: Whether to include inheritance relationships
    """
    # Load the feature catalogue
    catalogue = json.loads(feature_catalogue_path.read_text(encoding="utf-8"))
    
    if not isinstance(catalogue, Sequence) or isinstance(catalogue, (str, bytes)):
        raise TypeError("Feature catalogue must be a sequence of feature types")
    
    # Group features by package
    features_by_package: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    
    for feature in catalogue:
        if not isinstance(feature, Mapping):
            continue
        
        # Get the package name, default to "default" if not specified
        package = feature.get("package", "default")
        if not isinstance(package, str):
            package = str(package)
        
        features_by_package[package].append(feature)
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate a diagram for each package
    for package_name, features in sorted(features_by_package.items()):
        puml_output = render_feature_types_to_puml(
            features,
            title=f"Package: {package_name}",
            package=package_name,
            include_notes=include_notes,
            include_descriptions=include_descriptions,
            include_generalization=include_generalization,
        )
        
        # Create a safe filename from the package name
        safe_filename = "".join(
            c if c.isalnum() or c in "._- " else "_" 
            for c in package_name
        ).strip()
        safe_filename = safe_filename.replace(" ", "_")
        
        output_file = output_dir / f"{safe_filename}.puml"
        output_file.write_text(puml_output, encoding="utf-8")
        print(f"Generated: {output_file} ({len(features)} feature types)")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Generate multiple PlantUML diagrams grouped by package."
    )
    parser.add_argument(
        "catalogue",
        type=Path,
        help="Path to the feature_catalogue.json file"
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=Path("puml_output"),
        help="Output directory for generated .puml files (default: puml_output)"
    )
    parser.add_argument(
        "--no-notes",
        action="store_true",
        help="Disable inclusion of feature type notes"
    )
    parser.add_argument(
        "--no-descriptions",
        action="store_true",
        help="Disable attribute descriptions"
    )
    
    args = parser.parse_args()
    
    generate_puml_by_package(
        args.catalogue,
        args.output,
        include_notes=not args.no_notes,
        include_descriptions=not args.no_descriptions,
    )
