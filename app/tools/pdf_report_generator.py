from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from PIL import Image
from pathlib import Path

def create_report(images_folder, output_path, text_paragraph):
    """
    Create a PDF report with images and descriptive text.
    
    Args:
        images_folder (str or Path): Path to folder containing PNG images
        output_path (str or Path): Path where the PDF report will be saved
        text_paragraph (str): Descriptive text to include in the report
    """
    images_folder = Path(images_folder)
    output_path = Path(output_path)
    
    images_path = sorted(images_folder.glob("*.png"))
    
    c = canvas.Canvas(str(output_path), pagesize=A4)
    width, height = A4
    
    y = height - 2 * cm
    
    # ---- ADD IMAGES ----
    for img_path in images_path:
        img = Image.open(img_path)
        img_width, img_height = img.size
        
        aspect = img_height / img_width
        display_width = width - 4 * cm
        display_height = display_width * aspect
        
        if y - display_height < 2 * cm:
            c.showPage()
            y = height - 2 * cm
        
        c.drawImage(
            str(img_path),
            2 * cm,
            y - display_height,
            width=display_width,
            height=display_height
        )
        
        y -= display_height + 1 * cm
    
    # ---- ADD TEXT ----
    text_object = c.beginText(2 * cm, y)
    text_object.setFont("Helvetica", 11)
    
    for line in text_paragraph.split(". "):
        text_object.textLine(line)
    
    c.drawText(text_object)
    c.save()

# Usage example:
# create_report(
#     images_folder="/path/to/images",
#     output_path="/path/to/report.pdf",
#     text_paragraph="Your descriptive text here."
# )
