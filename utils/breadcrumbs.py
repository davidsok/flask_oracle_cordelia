from functools import wraps
from flask import g

def register_breadcrumb(name, url=None, parent=None, parent_url=None):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            try:
                resolved_url = url(**kwargs) if callable(url) else url
            except Exception as e:
                print(f"[ERROR] Resolving breadcrumb URL failed: {e}")
                resolved_url = ''

            try:
                resolved_parent_url = parent_url(**kwargs) if callable(parent_url) else parent_url
            except Exception as e:
                print(f"[ERROR] Resolving breadcrumb parent URL failed: {e}")
                resolved_parent_url = ''
            g.breadcrumbs = []
            if parent:
                g.breadcrumbs.append({'text': parent, 'url': resolved_parent_url})
            g.breadcrumbs.append({'text': name, 'url': resolved_url})
            return f(*args, **kwargs)
        return decorated_function
    return decorator
