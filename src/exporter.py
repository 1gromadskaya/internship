import json
import xml.etree.ElementTree as ET
from typing import Dict, List
from datetime import datetime
from decimal import Decimal


class Exporter:
    @staticmethod
    def to_json(data: Dict, output_path: str):

        def default_serializer(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            raise TypeError(f"Type {type(obj)} not serializable")

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=default_serializer)

        print(f"The result is saved in{output_path}")

    @staticmethod
    def to_xml(data: Dict, output_path: str):
        root = ET.Element("results")

        for query_name, rows in data.items():
            query_elem = ET.SubElement(root, "query")
            query_elem.set("name", query_name)

            for row in rows:
                row_elem = ET.SubElement(query_elem, "row")
                for key, value in row.items():
                    field_elem = ET.SubElement(row_elem, key)
                    if isinstance(value, (dict, list)):
                        field_elem.text = str(value)
                    elif value is None:
                        field_elem.text = ""
                    else:
                        field_elem.text = str(value)

        from xml.dom import minidom
        xml_str = ET.tostring(root, encoding='utf-8')
        dom = minidom.parseString(xml_str)
        pretty_xml = dom.toprettyxml(indent="  ")

        lines = [line for line in pretty_xml.split('\n') if line.strip()]

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))

        print(f"The result is saved in {output_path}")