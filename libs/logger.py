import logging.config
import os

os.makedirs('logs', exist_ok=True)

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
        'detailed': {
            'format': '%(asctime)s [%(levelname)s] %(name)s [%(filename)s:%(lineno)d]: %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'standard',
        },
        'file': {
            'class': 'logging.FileHandler',
            'filename': 'logs/app.log',  
            'level': 'DEBUG',
            'formatter': 'detailed',
        },
    },
    'loggers': {
        '': {  
            'handlers': ['console', 'file'],
            'level': 'DEBUG',
        },
    }
}

def setup_logging():
    logging.config.dictConfig(LOGGING_CONFIG)