"""
Docstring for app.tools.report_generator
"""
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
 
def generate_png_report(description_text:tuple, images_folder_path:str, output_path:str, output_name:str, num_weeks:int):
    # ---- CONFIG ----
    images_folder = Path(images_folder_path)
    canvas_width = 2600
    canvas_height = 1400
    padding = 30
    bg_color = "white"
    text_color = "black"

    # ---- LOAD IMAGES ----
    image_paths = sorted(images_folder.glob("*.png"))
    if len(image_paths) < 5:
        raise ValueError("At least 5 images are required.")

    images = [Image.open(p).convert("RGB") for p in image_paths]

    # ---- CANVAS ----
    canvas = Image.new("RGB", (canvas_width, canvas_height), bg_color)
    draw = ImageDraw.Draw(canvas)

    left_width = int(canvas_width * 0.55)
    right_width = canvas_width - left_width

    # ---- TEXT WRAPPING FUNCTION ----
    def wrap_text(text, font, max_width):
        words = text.split()
        lines = []
        line = ""

        dummy_img = Image.new("RGB", (1, 1))
        dummy_draw = ImageDraw.Draw(dummy_img)

        for word in words:
            test_line = f"{line}{word} "
            if dummy_draw.textlength(test_line, font=font) <= max_width:
                line = test_line
            else:
                lines.append(line)
                line = f"{word} "

        lines.append(line)
        return lines

    # ================= LEFT COLUMN =================
    # ---- FONT ----
    def get_font(size=24):
        return ImageFont.truetype("arial.ttf", size)
    
    text_max_width = left_width - 2 * padding
    # ---- TEXT ----
    title = description_text[0]
    employee_info = description_text[1]
    description = description_text[2]
    # ---- INSERT TITLE ----
    title_font_size = 50
    title_font = get_font(size=title_font_size)
    dummy_img = Image.new("RGB", (1, 1))
    dummy_draw = ImageDraw.Draw(dummy_img)
    title_x_coor = (text_max_width - dummy_draw.textlength(title, font=title_font)) // 2 + padding
    title_y_coor = padding
    draw.text((title_x_coor, title_y_coor), title, fill=text_color, font=title_font)
    # ---- INSERT EMPLOYEE INFO ----
    info_font_size = 28
    info_font = get_font(size=info_font_size)
    info_y_coor = title_y_coor + title_font_size + 30
    employee_info_chuncks = employee_info.split("|")
    for info in employee_info_chuncks:
        if ":" in info:
            label, value = info.split(":", 1)
            draw.text((padding, info_y_coor), label + ":", fill="black", font=info_font)
            label_width = info_font.getbbox(label + ":")[2]
            draw.text((padding + label_width, info_y_coor), value, fill="#990073", font=info_font)
        else:
            draw.text((padding, info_y_coor), info, fill="black", font=info_font)
        info_y_coor += info_font_size + 6
    # ---- INSERT DESCRIPTION ----
    desc_font_size = 22
    desc_font = get_font(size=desc_font_size)
    des_y_coor = info_y_coor + 10
    des_chuncks = description.split("|")
    for desc in des_chuncks:
        text_lines = wrap_text(desc, desc_font, text_max_width)
        for line in text_lines:
            draw.text((padding, des_y_coor), line, fill=text_color, font=desc_font)
            des_y_coor += desc_font_size + 6
        des_y_coor += 10  # Extra space between paragraphs

    # ---- BOTTOM IMAGE (LEFT) ----
    bottom_image = images[num_weeks]
    available_height = canvas_height - des_y_coor - padding
    img_ratio = text_max_width / bottom_image.width
    img_height = min(int(bottom_image.height * img_ratio), available_height)
    
    bottom_resized = bottom_image.resize((text_max_width, img_height))
    img_y = canvas_height - img_height - padding
    
    canvas.paste(bottom_resized, (padding, img_y))

    # ================= RIGHT COLUMN =================

    right_x = left_width + padding
    img_width = right_width - 2 * padding
    available_height = canvas_height - 2 * padding
    img_height = (available_height - 3 * padding) // num_weeks

    y = padding
    for img in images[:num_weeks]:
        ratio = img_width / img.width
        new_height = min(int(img.height * ratio), img_height)
        resized = img.resize((img_width, new_height))
        canvas.paste(resized, (right_x, y))
        y += img_height + padding

    # ---- SAVE ----
    canvas.save(f"{output_path}/{output_name}.png", format="PNG")
