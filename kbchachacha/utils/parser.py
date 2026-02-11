"""
HTML 파싱 유틸리티
"""
from __future__ import annotations
import re
from typing import Optional
from bs4 import BeautifulSoup


class HtmlParser:
    """HTML 파싱 헬퍼 클래스"""
    
    @staticmethod
    def remove_html_tags(html: str) -> str:
        """
        HTML 태그 제거
        
        Args:
            html: HTML 문자열
            
        Returns:
            태그가 제거된 텍스트
        """
        soup = BeautifulSoup(html, 'html.parser')
        if soup.pre:
            return soup.pre.text
        return soup.get_text()
    
    @staticmethod
    def parse_to_soup(html: str, parser: str = 'lxml') -> BeautifulSoup:
        """
        HTML을 BeautifulSoup 객체로 변환
        
        Args:
            html: HTML 문자열
            parser: 파서 종류 (기본값: lxml)
            
        Returns:
            BeautifulSoup 객체
        """
        return BeautifulSoup(html, parser)
    
    @staticmethod
    def extract_by_regex(text: str, pattern: str) -> Optional[str]:
        """
        정규식으로 텍스트 추출
        
        Args:
            text: 검색할 텍스트
            pattern: 정규식 패턴
            
        Returns:
            매칭된 텍스트 또는 None
        """
        # 줄바꿈 제거
        cleaned_text = text.replace("\n", "").replace("\r", "").replace("\t", "")
        
        match = re.search(pattern, cleaned_text)
        if match and match.groups():
            return match.group(1)
        
        return None
    
    @staticmethod
    def normalize_wheel_size(html: str) -> str:
        """
        휠 크기 표기 정규화
        
        Args:
            html: HTML 문자열
            
        Returns:
            정규화된 HTML
        """
        replacements = {
            '16"알로이휠': '16인치 알로이휠',
            '15"알로이휠': '15인치 알로이휠',
            '17"알로이휠': '17인치 알로이휠',
            '18"알로이휠': '18인치 알로이휠',
            '19"알로이휠': '19인치 알로이휠',
            '20"알로이휠': '20인치 알로이휠',
        }
        
        result = html
        for old, new in replacements.items():
            result = result.replace(old, new)
        
        return result
    
    @staticmethod
    def clean_html_wrapper(html: str) -> str:
        """
        HTML 래퍼 태그 제거
        
        Args:
            html: HTML 문자열
            
        Returns:
            정리된 HTML
        """
        return html.replace(
            "<html><head></head><body>", ""
        ).replace(
            "</body></html>", ""
        )
    
    @staticmethod
    def parse_og_description(html: str) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        """
        og:description 메타 태그에서 차량 정보 파싱
        
        Args:
            html: HTML 문자열
            
        Returns:
            (fullName, years, fuel, plateNumber) 튜플
        """
        # 차량번호 추출
        plate_pattern = r'<meta property="og:description" content="[ ]{0,10}\(([0-9ㄱ-ㅎ가-힣]{0,10})\)'
        plate_number = HtmlParser.extract_by_regex(html, plate_pattern)
        if plate_number:
            plate_number = plate_number.strip()
        
        # og:description content 추출
        meta_pattern = r'<meta\s+property="og:description"\s+content="([^"]+)"'
        meta_match = re.search(meta_pattern, html)
        
        if not meta_match:
            return None, None, None, plate_number
        
        content = meta_match.group(1)
        parts = [p.strip() for p in content.split('|')]
        
        if len(parts) < 4:
            return None, None, None, plate_number
        
        # fullName 추출
        fullname_match = re.search(r'\)(.+)', parts[0])
        full_name = fullname_match.group(1).strip() if fullname_match else None
        
        # years 추출 및 변환 (예: 24년형 -> 2024)
        years_match = re.search(r'(\d{2})년형', parts[1])
        years = f"20{years_match.group(1)}" if years_match else None
        
        # 연료 추출
        fuel = parts[3].strip() if len(parts) > 3 else None
        
        return full_name, years, fuel, plate_number