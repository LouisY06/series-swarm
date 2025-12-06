"""Agent C: vCard Contact Generator (Connector)."""

import logging
from io import BytesIO
from typing import Dict, Any
import vobject

logger = logging.getLogger(__name__)


def generate_vcard(contact_data: Dict[str, Any]) -> BytesIO:
    """
    Generate a vCard (.vcf file) for a contact.

    Args:
        contact_data: Dictionary containing contact information:
            - name: Contact name (required)
            - first_name: First name (optional, will use name if not provided)
            - last_name: Last name (optional)
            - email: Email address
            - phone: Phone number
            - organization: Organization/company
            - title: Job title
            - address: Address
            - url: Website URL

    Returns:
        BytesIO object containing the .vcf vCard file
    """
    try:
        # Create vCard object
        vcard = vobject.vCard()

        # Name handling
        name = contact_data.get('name', '')
        first_name = contact_data.get('first_name', '')
        last_name = contact_data.get('last_name', '')

        if not first_name and not last_name and name:
            # Try to split name if provided as full name
            name_parts = name.split(' ', 1)
            first_name = name_parts[0]
            last_name = name_parts[1] if len(name_parts) > 1 else ''

        # Add name
        vcard.add('n')
        vcard.n.value = vobject.vcard.Name(family=last_name, given=first_name)
        vcard.add('fn')
        vcard.fn.value = name or f"{first_name} {last_name}".strip()

        # Email
        email = contact_data.get('email')
        if email:
            vcard.add('email')
            vcard.email.value = email
            vcard.email.type_param = 'INTERNET'

        # Phone
        phone = contact_data.get('phone')
        if phone:
            vcard.add('tel')
            vcard.tel.value = phone
            vcard.tel.type_param = 'CELL'

        # Organization
        organization = contact_data.get('organization')
        if organization:
            vcard.add('org')
            vcard.org.value = [organization]

        # Title
        title = contact_data.get('title')
        if title:
            vcard.add('title')
            vcard.title.value = title

        # Address
        address = contact_data.get('address')
        if address:
            vcard.add('adr')
            vcard.adr.value = vobject.vcard.Address(street=address)

        # URL
        url = contact_data.get('url')
        if url:
            vcard.add('url')
            vcard.url.value = url

        # Convert to BytesIO
        vcf_string = vcard.serialize()
        vcard_io = BytesIO(vcf_string.encode('utf-8'))
        vcard_io.seek(0)

        logger.info(f"Generated vCard for contact: {name or first_name}")
        return vcard_io

    except Exception as e:
        logger.error(f"Error generating vCard: {e}")
        raise

