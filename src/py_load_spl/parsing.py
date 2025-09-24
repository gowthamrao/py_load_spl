# mypy: disable-error-code="assignment"
import logging
from pathlib import Path
from typing import Any

from lxml import etree

logger = logging.getLogger(__name__)


class SplParsingError(Exception):
    """Custom exception for errors during SPL file parsing."""

    def __init__(self, message: str, file_path: Path):
        self.file_path = file_path
        super().__init__(f"{message} [file: {file_path}]")


# Define the XML namespace for HL7 v3, crucial for XPath queries.
NAMESPACES = {"hl7": "urn:hl7-org:v3"}


def _xp(element: etree._Element, path: str) -> etree._Element | None:
    """Helper function for a single XPath query with the HL7 namespace."""
    return element.find(path, namespaces=NAMESPACES)


def _xpa(element: etree._Element, path: str) -> list[etree._Element]:
    """Helper function for multiple XPath queries with the HL7 namespace."""
    return element.findall(path, namespaces=NAMESPACES)


def parse_spl_file(file_path: Path) -> dict[str, Any]:
    """
    Parses a single SPL XML file to extract key information (F002).
    Uses lxml.etree.iterparse for memory-efficient parsing and is designed
    to be resilient to missing elements.
    """
    logger.debug(f"Parsing SPL file: {file_path}")

    try:
        raw_xml_content = file_path.read_text(encoding="utf-8")
    except OSError as e:
        logger.error(f"Could not read file {file_path}: {e}")
        raise

    try:
        # Use iterparse for memory-efficient parsing.
        # Security: Disable entity resolution to prevent XXE attacks.
        context = etree.iterparse(
            file_path,
            events=("end",),
            tag=f"{{{NAMESPACES['hl7']}}}document",
            resolve_entities=False,
        )
        _, root = next(context)
    except StopIteration:
        raise SplParsingError(
            "Could not find the root <document> element in the expected namespace.",
            file_path=file_path,
        ) from None
    except etree.XMLSyntaxError as e:
        # Catch parsing errors that can happen with iterparse, e.g., malformed XML
        logger.error(f"XML syntax error in {file_path}: {e}")
        raise SplParsingError(
            f"XML syntax error during parsing: {e}", file_path=file_path
        ) from e

    data: dict[str, Any] = {
        "ingredients": [],
        "packaging": [],
        "marketing_status": [],
        "product_ndcs": [],
    }

    try:
        # Extract metadata (F002.3)
        id_el = _xp(root, ".//hl7:id")
        data["document_id"] = id_el.get("root") if id_el is not None else None
        set_id_el = _xp(root, ".//hl7:setId")
        data["set_id"] = set_id_el.get("root") if set_id_el is not None else None
        version_el = _xp(root, ".//hl7:versionNumber")
        data["version_number"] = (
            int(version_el.get("value", 0)) if version_el is not None else 0
        )
        effective_time_el = _xp(root, ".//hl7:effectiveTime")
        data["effective_time"] = (
            effective_time_el.get("value") if effective_time_el is not None else None
        )
        data["raw_data"] = raw_xml_content
        data["source_filename"] = file_path.name

        # Locate the parent 'manufacturedProduct' element which holds both the product and manufacturer info
        parent_product_el = _xp(root, ".//hl7:manufacturedProduct")
        if parent_product_el is not None:
            product_element = _xp(parent_product_el, "./hl7:manufacturedProduct")
            if product_element is not None:
                # Extract product details
                product_name_el = _xp(product_element, ".//hl7:name")
                data["product_name"] = (
                    product_name_el.text if product_name_el is not None else None
                )

                dosage_form_el = _xp(product_element, ".//hl7:formCode")
                data["dosage_form"] = (
                    dosage_form_el.get("displayName")
                    if dosage_form_el is not None
                    else None
                )

                route_code_el = _xp(product_element, ".//hl7:routeCode")
                data["route_of_administration"] = (
                    route_code_el.get("displayName")
                    if route_code_el is not None
                    else None
                )

                # Extract Product NDCs
                as_equivalent_entity_el = _xp(
                    product_element, "./hl7:asEquivalentEntity"
                )
                if as_equivalent_entity_el is not None:
                    for code_el in _xpa(
                        as_equivalent_entity_el,
                        "./hl7:code[@codeSystem='2.16.840.1.113883.6.69']",
                    ):
                        if ndc_code := code_el.get("code"):
                            data["product_ndcs"].append({"ndc_code": ndc_code})

                # Extract ingredients (F003.2)
                for ingredient_el in _xpa(product_element, ".//hl7:ingredient"):
                    substance = _xp(ingredient_el, ".//hl7:ingredientSubstance")
                    quantity = _xp(ingredient_el, ".//hl7:quantity")
                    numerator = (
                        _xp(quantity, ".//hl7:numerator")
                        if quantity is not None
                        else None
                    )
                    denominator = (
                        _xp(quantity, ".//hl7:denominator")
                        if quantity is not None
                        else None
                    )
                    substance_name_el = (
                        _xp(substance, ".//hl7:name") if substance is not None else None
                    )
                    substance_code_el = (
                        _xp(substance, ".//hl7:code") if substance is not None else None
                    )
                    data["ingredients"].append(
                        {
                            "ingredient_name": substance_name_el.text
                            if substance_name_el is not None
                            else None,
                            "substance_code": substance_code_el.get("code")
                            if substance_code_el is not None
                            else None,
                            "is_active_ingredient": ingredient_el.get("classCode")
                            == "ACT",
                            "strength_numerator": numerator.get("value")
                            if numerator is not None
                            else None,
                            "strength_denominator": denominator.get("value")
                            if denominator is not None
                            else None,
                            "unit_of_measure": numerator.get("unit")
                            if numerator is not None
                            else None,
                        }
                    )

            # Extract manufacturer from the parent element
            manufacturer_el = _xp(parent_product_el, "./hl7:manufacturer/hl7:name")
            data["manufacturer_name"] = (
                manufacturer_el.text if manufacturer_el is not None else None
            )

        # Extract from the structured body for packaging and marketing status
        body = _xp(root, ".//hl7:component/hl7:structuredBody")
        if body is not None:
            # Packaging (F003.2)
            for section in _xpa(body, ".//hl7:section"):
                code_el = _xp(section, ".//hl7:code")
                if code_el is not None and code_el.get("code") in (
                    "34069-5",
                    "51945-4",
                ):
                    for part_el in _xpa(section, ".//hl7:part"):
                        part_code_el = _xp(part_el, "./hl7:code")
                        if part_code_el is not None:
                            desc_el = _xp(part_el, "./hl7:name")
                            if desc_el is None:
                                desc_el = _xp(part_el, "./hl7:desc")
                            form_code_el = _xp(part_el, "./hl7:formCode")
                            data["packaging"].append(
                                {
                                    "package_ndc": part_code_el.get("code"),
                                    "package_description": desc_el.text
                                    if desc_el is not None
                                    else None,
                                    "package_type": form_code_el.get("displayName")
                                    if form_code_el is not None
                                    else None,
                                }
                            )

            # Marketing Status
            for act in _xpa(body, ".//hl7:subject//hl7:marketingAct"):
                status_code_el = _xp(act, "./hl7:statusCode")
                effective_time_el = _xp(act, "./hl7:effectiveTime")
                start_date_el = (
                    _xp(effective_time_el, "./hl7:low")
                    if effective_time_el is not None
                    else None
                )
                end_date_el = (
                    _xp(effective_time_el, "./hl7:high")
                    if effective_time_el is not None
                    else None
                )
                data["marketing_status"].append(
                    {
                        "marketing_category": status_code_el.get("code")
                        if status_code_el is not None
                        else None,
                        "start_date": start_date_el.get("value")
                        if start_date_el is not None
                        else None,
                        "end_date": end_date_el.get("value")
                        if end_date_el is not None
                        else None,
                    }
                )

    except (AttributeError, TypeError, ValueError) as e:
        logger.error(
            f"Error parsing file {file_path}. Some elements may be missing. Error: {e}"
        )
        raise SplParsingError(
            f"A critical error occurred during parsing: {e}", file_path=file_path
        ) from e

    # Free up memory
    root.clear()
    while root.getprevious() is not None:
        del root.getparent()[0]

    return data
