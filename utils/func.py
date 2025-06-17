import os
from config import Config

def get_file_path(filename):
    return os.path.join(Config.UPLOAD_FOLDER, filename) + '.csv'

def allowed_file(filename):
    print('FILENAME', filename.rsplit('.', 1)[1].lower())
    print(Config.ALLOWED_EXTENSIONS)
    print(filename.rsplit('.', 1)[1].lower() in Config.ALLOWED_EXTENSIONS)
    return filename.rsplit('.', 1)[1].lower() in Config.ALLOWED_EXTENSIONS

def percentage_converter(x):
    try:
        if isinstance(x, str) and '%' in x:
            return float(x.replace('%', '').strip()) / 100
        return float(x)
    except (ValueError, TypeError):
        return None