"""
크롤러 메인 실행 파일
"""
from __future__ import annotations
import sys
from services.crawler_service import CrawlerService
from utils.logger import get_logger

logger = get_logger()


def main():
    """메인 함수"""
    try:
        # 크롤러 서비스 생성 및 실행
        crawler = CrawlerService()
        crawler.run()
        
        return 0
    
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
        return 1
    
    except Exception as e:
        logger.critical(f"프로그램 오류: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())