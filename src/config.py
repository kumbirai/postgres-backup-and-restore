import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
load_dotenv()

# Get the base directory (project root, one level up from src)
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class Config:
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'keap_db')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'secret')
    
    # Backup settings
    BACKUP_DIR = os.getenv('BACKUP_DIR', os.path.join(base_dir, 'backups'))
    COMPRESSION_FORMAT = os.getenv('COMPRESSION_FORMAT', 'gzip')  # gzip or custom
    
    @classmethod
    def get_db_url(cls) -> str:
        """Get database connection URL."""
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
    
    @classmethod
    def ensure_backup_dir(cls) -> None:
        """Ensure backup directory exists."""
        Path(cls.BACKUP_DIR).mkdir(parents=True, exist_ok=True) 