"""
데이터베이스 커넥션 풀 관리
"""
from __future__ import annotations

import pymysql
from pymysql.cursors import DictCursor
from contextlib import contextmanager
from typing import Generator
from config.settings import db_config


class DatabasePool:
    """데이터베이스 커넥션 풀 관리 클래스"""
    
    def __init__(self):
        self._pool = []
        self._pool_size = db_config.POOL_SIZE
        self._initialize_pool()
    
    def _initialize_pool(self):
        """커넥션 풀 초기화"""
        for _ in range(self._pool_size):
            conn = self._create_connection()
            self._pool.append(conn)
    
    def _create_connection(self):
        """새로운 DB 연결 생성"""
        return pymysql.connect(
            host=db_config.HOST,
            port=db_config.PORT,
            user=db_config.USER,
            passwd=db_config.PASSWORD,
            db=db_config.DATABASE,
            charset=db_config.CHARSET,
            cursorclass=DictCursor,
            autocommit=False
        )
    
    def get_connection(self):
        """풀에서 커넥션 가져오기"""
        if self._pool:
            conn = self._pool.pop()
            # 연결 확인
            try:
                conn.ping(reconnect=True)
                return conn
            except:
                return self._create_connection()
        return self._create_connection()
    
    def return_connection(self, conn):
        """커넥션을 풀에 반환"""
        if len(self._pool) < self._pool_size:
            try:
                conn.ping(reconnect=True)
                self._pool.append(conn)
            except:
                conn.close()
        else:
            conn.close()
    
    def close_all(self):
        """모든 커넥션 종료"""
        while self._pool:
            conn = self._pool.pop()
            try:
                conn.close()
            except:
                pass


# 싱글톤 인스턴스
_db_pool = None


def get_db_pool() -> DatabasePool:
    """DB 풀 싱글톤 인스턴스 반환"""
    global _db_pool
    if _db_pool is None:
        _db_pool = DatabasePool()
    return _db_pool


@contextmanager
def get_db_connection() -> Generator:
    """
    컨텍스트 매니저로 DB 연결 관리
    
    사용예:
        with get_db_connection() as (conn, cursor):
            cursor.execute("SELECT * FROM table")
            result = cursor.fetchall()
    """
    pool = get_db_pool()
    conn = pool.get_connection()
    cursor = None
    
    try:
        cursor = conn.cursor()
        yield conn, cursor
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()
        pool.return_connection(conn)


def execute_query(query: str, params: tuple = None):
    """
    쿼리 실행 헬퍼 함수
    
    Args:
        query: SQL 쿼리
        params: 쿼리 파라미터
    
    Returns:
        쿼리 결과
    """
    with get_db_connection() as (conn, cursor):
        cursor.execute(query, params)
        return cursor.fetchall()


def execute_update(query: str, params: tuple = None) -> int:
    """
    UPDATE/INSERT 실행 헬퍼 함수
    
    Args:
        query: SQL 쿼리
        params: 쿼리 파라미터
    
    Returns:
        영향받은 행 수
    """
    with get_db_connection() as (conn, cursor):
        affected = cursor.execute(query, params)
        return affected