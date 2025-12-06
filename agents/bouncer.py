"""Agent A: PDF Ticket Generator (Bouncer)."""

import logging
from io import BytesIO
from typing import Dict, Any
from fpdf import FPDF

logger = logging.getLogger(__name__)


class TicketPDF(FPDF):
    """Custom PDF class for event tickets."""

    def header(self):
        """Draw ticket header."""
        self.set_font('Arial', 'B', 16)
        self.cell(0, 10, 'EVENT TICKET', 0, 1, 'C')
        self.ln(5)

    def footer(self):
        """Draw ticket footer."""
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')


def generate_ticket(event_data: Dict[str, Any]) -> BytesIO:
    """
    Generate a PDF ticket for an event.

    Args:
        event_data: Dictionary containing event information:
            - title: Event title
            - date: Event date
            - time: Event time
            - location: Event location
            - ticket_number: Optional ticket number
            - attendee_name: Optional attendee name

    Returns:
        BytesIO object containing the PDF ticket
    """
    try:
        pdf = TicketPDF()
        pdf.add_page()

        # Extract event data with defaults
        title = event_data.get('title', 'Event')
        date = event_data.get('date', 'TBD')
        time = event_data.get('time', 'TBD')
        location = event_data.get('location', 'TBD')
        ticket_number = event_data.get('ticket_number', 'N/A')
        attendee_name = event_data.get('attendee_name', 'Guest')

        # Title
        pdf.set_font('Arial', 'B', 20)
        pdf.cell(0, 15, title, 0, 1, 'C')
        pdf.ln(5)

        # Event details
        pdf.set_font('Arial', '', 12)
        pdf.cell(0, 8, f'Date: {date}', 0, 1, 'L')
        pdf.cell(0, 8, f'Time: {time}', 0, 1, 'L')
        pdf.cell(0, 8, f'Location: {location}', 0, 1, 'L')
        pdf.ln(5)

        # Ticket number
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 8, f'Ticket #: {ticket_number}', 0, 1, 'L')
        pdf.ln(5)

        # Attendee name
        pdf.set_font('Arial', '', 12)
        pdf.cell(0, 8, f'Attendee: {attendee_name}', 0, 1, 'L')
        pdf.ln(10)

        # Barcode area (placeholder)
        pdf.set_font('Arial', 'I', 10)
        pdf.cell(0, 8, 'Present this ticket at the event', 0, 1, 'C')

        # Convert to BytesIO
        pdf_bytes = pdf.output(dest='S').encode('latin-1')
        ticket_io = BytesIO(pdf_bytes)
        ticket_io.seek(0)

        logger.info(f"Generated PDF ticket for event: {title}")
        return ticket_io

    except Exception as e:
        logger.error(f"Error generating ticket: {e}")
        raise

