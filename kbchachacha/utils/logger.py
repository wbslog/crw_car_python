import logging
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler


def get_logger(name: str = None) -> logging.Logger:
    """
    로거 인스턴스 반환
    
    Args:
        name: 로거 이름 (None이면 root logger)
        
    Returns:
        Logger 인스턴스
    """
    # 로거 생성
    logger = logging.getLogger(name or __name__)
    
    # 이미 핸들러가 있으면 재사용
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.DEBUG)
    
    # 운영체제별 로그 디렉토리 설정
    if sys.platform.startswith('linux'):
        log_dir = "/data/niffler_kb/logs"
    else:
        log_dir = "logs"  # Windows 로컬 개발용
    
    # 로그 디렉토리 생성
    try:
        os.makedirs(log_dir, exist_ok=True)
    except PermissionError:
        # 권한 문제 시 현재 디렉토리에 생성
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        print(f"Warning: 로그 디렉토리 권한 없음. {log_dir}에 저장합니다.")
    
    # 로그 파일명 (날짜별)
    today = datetime.now().strftime('%Y%m%d')
    log_file = os.path.join(log_dir, f'crawler_{today}.log')
    error_log_file = os.path.join(log_dir, f'error_{today}.log')
    
    # 포맷 설정
    formatter = logging.Formatter(
        '[%(asctime)s][%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    detailed_formatter = logging.Formatter(
        '[%(asctime)s][%(levelname)s][%(name)s:%(lineno)d] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 파일 핸들러 - 전체 로그 (최대 50MB, 10개 백업)
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=50*1024*1024,  # 50MB
        backupCount=10,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    
    # 파일 핸들러 - 에러 로그만 (최대 10MB, 5개 백업)
    error_handler = RotatingFileHandler(
        error_log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(detailed_formatter)
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # 핸들러 추가
    logger.addHandler(file_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)
    
    # 로그 파일 경로 출력
    logger.info(f"로그 파일: {log_file}")
    
    return logger