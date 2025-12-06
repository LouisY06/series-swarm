"""vCard generator for contact sharing."""

import logging
from io import BytesIO
from typing import Dict, Any
import vobject

logger = logging.getLogger(__name__)


def generate_vcard(contact_data: Dict[str, Any]) -> BytesIO:
    """
    Generate a vCard (.vcf file) from contact data.
    
    Args:
        contact_data: Dict with name, email, phone, etc.
    
    Returns:
        BytesIO containing the vCard file
    """
    try:
        vcard = vobject.vCard()

        # Get name data
        name = contact_data.get('name', '').strip()
        phone = contact_data.get('phone', '').strip()
        email = contact_data.get('email', '').strip()

        # Use phone as fallback name
        if not name:
            if phone:
                name = f"User {phone[-4:]}"
            else:
                name = "Stranger"

        # Split name into first/last
        name_parts = name.split(' ', 1)
        first_name = name_parts[0]
        last_name = name_parts[1] if len(name_parts) > 1 else ''

        # Add name fields (required by vCard spec)
        vcard.add('n')
        vcard.n.value = vobject.vcard.Name(family=last_name, given=first_name)
        vcard.add('fn')
        vcard.fn.value = name

        # Add phone
        if phone:
            vcard.add('tel')
            vcard.tel.value = phone
            vcard.tel.type_param = 'CELL'

        # Add email
        if email:
            vcard.add('email')
            vcard.email.value = email
            vcard.email.type_param = 'INTERNET'

        # Add notes with interests/hobbies and other info
        notes_parts = []
        interests = contact_data.get('interests', '').strip()
        bio = contact_data.get('bio', '').strip()
        
        if interests:
            notes_parts.append(f"Interests: {interests}")
        if bio:
            notes_parts.append(bio)
        
        # Add any other fields that might be useful
        for key, value in contact_data.items():
            if key not in ['name', 'phone', 'email', 'interests', 'bio'] and value:
                notes_parts.append(f"{key.capitalize()}: {value}")
        
        if notes_parts:
            vcard.add('note')
            vcard.note.value = '\n'.join(notes_parts)

        # Serialize to BytesIO
        vcf_string = vcard.serialize()
        vcard_io = BytesIO(vcf_string.encode('utf-8'))
        vcard_io.seek(0)

        logger.info(f"Generated vCard for: {name}")
        return vcard_io

    except Exception as e:
        logger.error(f"Error generating vCard: {e}")
        raise

