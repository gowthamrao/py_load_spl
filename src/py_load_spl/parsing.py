import io
import logging
from collections.abc import Generator
from pathlib import Path
from typing import Any, cast

from lxml import etree

logger = logging.getLogger(__name__)

# Define the XML namespace for HL7 v3
# This is crucial for XPath queries to work correctly.
NAMESPACES = {"hl7": "urn:hl7-org:v3"}


def _xp(element: etree._Element, path: str) -> Any | None:
    """
    Helper function to perform an XPath query with the HL7 namespace.
    Returns the first result or None if not found.
    """
    return element.find(path, namespaces=NAMESPACES)


def _xpa(element: etree._Element, path: str) -> list[etree._Element]:
    """
    Helper function to perform an XPath query with the HL7 namespace.
    Returns all results or an empty list if not found.
    """
    return element.findall(path, namespaces=NAMESPACES)


def parse_spl_file(file_path: Path) -> dict[str, Any]:
    """
    Parses a single SPL XML file to extract key information (F002).

    This function uses `lxml.etree.iterparse` for memory-efficient parsing
    and is designed to be resilient to missing elements.

    Args:
        file_path: The path to the SPL XML file.

    Returns:
        A dictionary containing the extracted data.
    """
    logger.debug(f"Parsing SPL file: {file_path}")
    # F004.1: Capture the complete, original XML data
    raw_xml = file_path.read_text(encoding="utf-8")
    context = etree.iterparse(
        io.BytesIO(raw_xml.encode("utf-8")),
        events=("end",),
        tag=f"{{{NAMESPACES['hl7']}}}document",
    )

    # Since we expect only one <document> element, we process the first one we find.
    _, root = next(context)

    data: dict[str, Any] = {
        "raw_xml": raw_xml,
        "source_filename": file_path.name,
        "product_ndcs": [],
        "ingredients": [],
        "packaging": [],
        "marketing_status": [],
    }

    try:
        # F002.3: Extract metadata
        data["document_id"] = _xp(root, ".//hl7:id").get("root")
        data["set_id"] = _xp(root, ".//hl7:setId").get("root")
        data["version_number"] = int(_xp(root, ".//hl7:versionNumber").get("value", 0))
        data["effective_time"] = _xp(root, ".//hl7:effectiveTime").get("value")

        # Extract product details
        product_element = _xp(root, ".//hl7:manufacturedProduct/hl7:manufacturedProduct")
        if product_element is not None:
            data["product_name"] = _xp(product_element, ".//hl7:name").text
            data["dosage_form"] = _xp(product_element, ".//hl7:formCode").get(
                "displayName"
            )
            # F002.3: Extract route of administration (best guess)
            route_code_el = _xp(product_element, ".//hl7:routeCode")
            if route_code_el is not None:
                data["route_of_administration"] = route_code_el.get("displayName")

            # Extract the Product NDC code
            # Based on assumption that it's a direct child of manufacturedProduct
            code_el = _xp(product_element, ".//hl7:code")
            if code_el is not None:
                ndc_code = code_el.get("code")
                if ndc_code:
                    data["product_ndcs"].append({"ndc_code": ndc_code})

        manufacturer = _xp(root, ".//hl7:manufacturer/hl7:name")
        if manufacturer is not None:
            data["manufacturer_name"] = manufacturer.text

        # Extract ingredients (F003.2)
        for ingredient_el in _xpa(product_element, ".//hl7:ingredient"):
            substance = _xp(ingredient_el, ".//hl7:ingredientSubstance")
            quantity = _xp(ingredient_el, ".//hl7:quantity")
            numerator = _xp(quantity, ".//hl7:numerator")
            denominator = _xp(quantity, ".//hl7:denominator")

            data["ingredients"].append(
                {
                    "ingredient_name": _xp(substance, ".//hl7:name").text,
                    "substance_code": _xp(substance, ".//hl7:code").get("code"),
                    "is_active_ingredient": ingredient_el.get("classCode") == "ACT",
                    "strength_numerator": numerator.get("value"),
                    "strength_denominator": denominator.get("value"),
                    "unit_of_measure": numerator.get("unit"),
                }
            )

        # Extract packaging and marketing status from the structured body
        body = _xp(root, ".//hl7:component/hl7:structuredBody")
        if body is not None:
            # Find the packaging section by iterating through sections
            packaging_section = None
            for section in _xpa(body, ".//hl7:section"):
                # The code element is a direct child of the section
                code_el = _xp(section, "hl7:code")
                if code_el is not None and code_el.get("code") == "51945-4":
                    packaging_section = section
                    break  # Found it

            if packaging_section is not None:
                # Based on investigation, packaging information is often in `part` -> `containerPackage`
                for part_el in _xpa(packaging_section, ".//hl7:part"):
                    pkg = _xp(part_el, ".//hl7:containerPackage")
                    if pkg is None:
                        continue

                    code_el = _xp(pkg, ".//hl7:code")
                    name_el = _xp(pkg, ".//hl7:name")
                    form_code_el = _xp(pkg, ".//hl7:formCode")

                    data["packaging"].append(
                        {
                            "package_ndc": code_el.get("code") if code_el is not None else None,
                            "package_description": name_el.text if name_el is not None else None,
                            "package_type": form_code_el.get("displayName") if form_code_el is not None else None,
                        }
                    )

                # Extract marketing status
                marketing_act = _xp(packaging_section, ".//hl7:marketingAct")
                if marketing_act is not None:
                    status_code = _xp(marketing_act, ".//hl7:statusCode")
                    effective_time = _xp(marketing_act, ".//hl7:effectiveTime/hl7:low")
                    data["marketing_status"].append(
                        {
                            "marketing_category": status_code.get("code")
                            if status_code is not None
                            else None,
                            "start_date": effective_time.get("value")
                            if effective_time is not None
                            else None,
                        }
                    )

    except (AttributeError, TypeError, ValueError) as e:
        # F002.4: Gracefully handle parsing errors
        logger.error(
            f"Error parsing file {file_path}. Some elements may be missing. Error: {e}"
        )

    # Clear the root element to free memory
    root.clear()
    # Also clear the parent of the root element
    while root.getprevious() is not None:
        del root.getparent()[0]

    return data


def iter_spl_files(source_dir: Path) -> Generator[dict[str, Any], None, None]:
    """
    A generator that finds all XML files in a directory, parses them,
    and yields the structured data.
    """
    logger.info(f"Searching for SPL XML files in {source_dir}...")
    xml_files = list(source_dir.glob("**/*.xml"))
    if not xml_files:
        logger.warning(f"No XML files found in {source_dir}")
        return

    for xml_file in xml_files:
        try:
            yield parse_spl_file(xml_file)
        except Exception as e:
            # F002.4: Log error and move to quarantine (not implemented yet)
            logger.error(f"Failed to parse {xml_file}, skipping. Error: {e}")
            # In a real implementation, we would move the file here.
            pass
    logger.info(f"Finished processing {len(xml_files)} SPL files.")
