"""
프록시 관리 유틸리티
"""
from __future__ import annotations
from typing import Dict
from config.database import execute_query, execute_update
from config.settings import proxy_config
from utils.logger import get_logger

logger = get_logger()


class ProxyManager:
    """프록시 서버 관리 클래스"""
    
    @staticmethod
    def get_least_used_proxy() -> Dict[str, str]:
        """
        사용 빈도가 가장 낮은 프록시 정보 가져오기
        
        Returns:
            프록시 정보 딕셔너리
        """
        logger.info("프록시 서버 정보 조회 중...")
        
        query = """
            SELECT 
                PROXY_CODE,
                PROXY_IP,
                PROXY_PORT,
                EXEC_COUNT
            FROM NIF_PROXY_LIST 
            WHERE STATUS = '1'
            ORDER BY EXEC_COUNT ASC
            LIMIT 1
        """
        
        result = execute_query(query)
        
        if not result:
            raise ValueError("사용 가능한 프록시 서버가 없습니다")
        
        proxy_data = result[0]
        
        # 실행 카운트 증가
        update_query = """
            UPDATE NIF_PROXY_LIST
            SET EXEC_COUNT = EXEC_COUNT + 1
            WHERE PROXY_CODE = %s
        """
        execute_update(update_query, (proxy_data['PROXY_CODE'],))
        
        proxy_info = {
            'PROXY_CODE': proxy_data['PROXY_CODE'],
            'PROXY_IP': proxy_data['PROXY_IP'],
            'PROXY_PORT': str(proxy_data['PROXY_PORT']),
            'PROXY_USER_ID': proxy_config.USER_ID,
            'PROXY_USER_PASS': proxy_config.PASSWORD
        }
        
        logger.info(f"프록시 정보 획득 - IP: {proxy_info['PROXY_IP']}, PORT: {proxy_info['PROXY_PORT']}")
        
        return proxy_info
    
    @staticmethod
    def get_proxy_wire_options(proxy_info: Dict[str, str]) -> Dict:
        """
        Selenium Wire용 프록시 옵션 생성
        
        Args:
            proxy_info: 프록시 정보
            
        Returns:
            seleniumwire 옵션 딕셔너리
        """
        proxy_url = (
            f"socks5://{proxy_info['PROXY_USER_ID']}:{proxy_info['PROXY_USER_PASS']}"
            f"@{proxy_info['PROXY_IP']}:{proxy_info['PROXY_PORT']}"
        )
        
        return {
            'proxy': {
                'http': proxy_url,
                'https': proxy_url,
                'no_proxy': 'localhost,127.0.0.1'
            }
        }