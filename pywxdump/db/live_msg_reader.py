# -*- coding: utf-8 -*-#
# -------------------------------------------------------------------------------
# Name:         live_msg_reader.py
# Description:  通过 SQLCipher 直连源 MSG 库读取实时消息
# Author:       xaoyaoo
# Date:         2026/02/10
# -------------------------------------------------------------------------------
import os
from typing import Dict, List, Optional, Tuple

from pywxdump import get_core_db


class LiveMsgReader:
    """读取原始 MSG*.db（含 WAL 最新快照）并返回与 DBHandler.get_msgs 兼容的数据结构。"""

    def __init__(self, wx_path: str, key: str, formatter, cipher_compatibility: int = 3):
        self.wx_path = wx_path
        self.key = key
        self.formatter = formatter
        self.cipher_compatibility = cipher_compatibility

    @staticmethod
    def _load_sqlcipher_driver():
        drivers = [
            ("sqlcipher3", "dbapi2"),
            ("pysqlcipher3", "dbapi2"),
            ("pysqlcipher3", None),
        ]
        last_error = None
        for module_name, attr in drivers:
            try:
                module = __import__(module_name, fromlist=[attr] if attr else [])
                return getattr(module, attr) if attr else module
            except Exception as e:
                last_error = e
        raise ImportError(f"未找到可用 SQLCipher 驱动: {last_error}")

    def _query_live_rows(self, wxids: List[str], start_index: int, page_size: int,
                         start_createtime: Optional[float], end_createtime: Optional[float]) -> List[tuple]:
        ok, db_infos = get_core_db(self.wx_path, ["MSG"])
        if not ok:
            raise RuntimeError(db_infos)

        db_paths = [item["db_path"] for item in db_infos if os.path.exists(item.get("db_path", ""))]
        if not db_paths:
            return []

        sqlcipher = self._load_sqlcipher_driver()
        main_path = db_paths[0].replace("'", "''")
        conn = sqlcipher.connect(main_path)
        try:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA key = \"x'{self.key}'\";")
            cursor.execute(f"PRAGMA cipher_compatibility = {int(self.cipher_compatibility)};")
            cursor.execute("PRAGMA journal_mode;")  # 触发连接初始化，确保 WAL 快照可见

            aliases = ["main"]
            for idx, db_path in enumerate(db_paths[1:], start=1):
                alias = f"msg_{idx}"
                db_path_safe = db_path.replace("'", "''")
                cursor.execute(f"ATTACH DATABASE '{db_path_safe}' AS {alias} KEY \"x'{self.key}'\";")
                cursor.execute(f"PRAGMA {alias}.cipher_compatibility = {int(self.cipher_compatibility)};")
                aliases.append(alias)

            where_clauses = ["1=1"]
            params: List = []
            if wxids:
                where_clauses.append(f"StrTalker IN ({', '.join('?' for _ in wxids)})")
                params.extend(wxids)
            if start_createtime:
                where_clauses.append("CreateTime >= ?")
                params.append(start_createtime)
            if end_createtime:
                where_clauses.append("CreateTime <= ?")
                params.append(end_createtime)
            where_sql = " AND ".join(where_clauses)

            select_fields = (
                "localId,TalkerId,MsgSvrID,Type,SubType,CreateTime,IsSender,Sequence,StatusEx,FlagEx,Status,"
                "MsgSequence,StrContent,MsgServerSeq,StrTalker,DisplayContent,Reserved0,Reserved1,Reserved3,"
                "Reserved4,Reserved5,Reserved6,CompressContent,BytesExtra,BytesTrans,Reserved2"
            )
            union_sql = " UNION ALL ".join(
                [f"SELECT {select_fields} FROM {alias}.MSG WHERE {where_sql}" for alias in aliases]
            )
            sql = (
                "SELECT *, ROW_NUMBER() OVER (ORDER BY CreateTime ASC) AS id "
                f"FROM ({union_sql}) m "
                "ORDER BY CreateTime ASC LIMIT ?, ?"
            )
            final_params = tuple(params * len(aliases) + [start_index, page_size])
            cursor.execute(sql, final_params)
            rows = cursor.fetchall()
            return rows or []
        finally:
            conn.close()

    def get_msgs(self, wxids: str or List[str] = "", start_index: int = 0, page_size: int = 500,
                 start_createtime: Optional[float] = None, end_createtime: Optional[float] = None,
                 my_talker: str = "我") -> Tuple[List[Dict], Dict]:
        if isinstance(wxids, str) and wxids:
            wxids = [wxids]
        wxids = wxids or []

        rows = self._query_live_rows(wxids=wxids, start_index=start_index, page_size=page_size,
                                     start_createtime=start_createtime, end_createtime=end_createtime)
        msgs = [self.formatter.get_msg_detail(row, my_talker=my_talker) for row in rows]
        wxid_list = {item.get("talker") for item in msgs if item.get("talker")}
        users = self.formatter.get_user(wxids=list(wxid_list)) if wxid_list else {}
        return msgs, users
