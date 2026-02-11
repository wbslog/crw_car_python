"""
차량 정보 데이터 모델
"""

from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class CarInfo:
    """차량 정보 데이터 클래스"""
    
    # 필수 필드
    prd_seq: str
    site_code: str
    car_id: str
    detail_url: str = ""
    
    # 기본 정보
    ap_model_id: str = ""
    plate_number: str = ""
    full_name: str = ""
    
    # 차량 상세 정보
    color: str = ""
    new_price: str = ""
    make_price: str = ""
    vin_number: str = ""
    kind: str = ""
    domestic: str = ""
    maker: str = ""
    model: str = ""
    model_detail: str = ""
    grade: str = ""
    grade_detail: str = ""
    years: str = ""
    fuel: str = ""
    mission: str = ""
    
    # 동기화 상태
    sync_status: str = "1"
    sync_text: str = ""
    
    def to_dict(self) -> dict:
        """딕셔너리로 변환"""
        return asdict(self)
    
    def mark_as_completed(self):
        """VIP 완료 상태로 표시"""
        self.sync_status = "3"
        self.sync_text = "VIP Complete"
    
    def mark_as_carmart_synced(self):
        """카마트 동기화 완료 상태로 표시"""
        self.sync_status = "3"
        self.sync_text = "VIP Complete"
    
    def mark_as_sync_failed(self, reason: str = "CAR-MART Sync Fail(Not Found)"):
        """동기화 실패 상태로 표시"""
        self.sync_status = "9"
        self.sync_text = reason
    
    def mark_as_closed(self, reason: str = "차량이 존재하지않음(삭제됨)"):
        """종료 상태로 표시"""
        self.sync_status = "9"
        self.sync_text = reason
    
    def update_from_carmart(self, carmart_data: dict):
        """카마트 API 응답으로 정보 업데이트"""
        self.ap_model_id = carmart_data.get("apModelId", "")
        self.color = carmart_data.get("color", "")
        self.new_price = carmart_data.get("newPrice", "")
        self.make_price = carmart_data.get("carMakePrice", "")
        self.vin_number = carmart_data.get("vinCode", "")
        self.kind = carmart_data.get("kindName", "")
        self.domestic = carmart_data.get("carDomestic", "")
        self.maker = carmart_data.get("makerName", "")
        self.model = carmart_data.get("modelName", "")
        self.model_detail = carmart_data.get("modelDetailName", "")
        self.grade = carmart_data.get("gradeName", "")
        self.grade_detail = carmart_data.get("gradeDetailName", "")
        self.years = carmart_data.get("carYear", "")
        self.fuel = carmart_data.get("fuel", "")
        self.mission = carmart_data.get("gearBox", "")
    
    def clear_carmart_data(self):
        """카마트 데이터 초기화"""
        self.ap_model_id = ""
        self.color = ""
        self.new_price = ""
        self.make_price = ""
        self.vin_number = ""
        self.kind = ""
        self.domestic = ""
        self.maker = ""
        self.model = ""
        self.model_detail = ""
        self.grade = ""
        self.grade_detail = ""
        self.years = ""
        self.fuel = ""
        self.mission = ""