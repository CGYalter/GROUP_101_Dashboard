import os
import datetime as dt
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

from sqlalchemy import create_engine, text
from pymongo import MongoClient

load_dotenv()

#Postgres schema helper
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")   # CHANGE: "public" to your own schema name
def qualify(sql: str) -> str:
    # Replace occurrences of {S}.<table> with <schema>.<table>
    return sql.replace("{S}.", f"{PG_SCHEMA}.")

# CONFIG: Postgres and Mongo Queries
CONFIG = {
    "postgres": {
        "enabled": True,
        "uri": os.getenv("PG_URI", "postgresql+psycopg2://postgres:password@localhost:5432/postgres"),  # Will read from your .env file
        "queries": {
            # 患者角色查询
            "患者: 我的健康数据概览 (表格)": {
                "sql": """
                    SELECT hr.timestamp, hr.heartrate, hr.bloodpressure, 
                           hr.spo2, hr.stepcount, wd.devicetype
                    FROM {S}.healthrecord hr
                    JOIN {S}.wearabledevice wd ON hr.deviceid = wd.deviceid
                    WHERE wd.assignedpatientid = :patient_id
                    ORDER BY hr.timestamp DESC
                    LIMIT 20;
                """,
                "chart": {"type": "table"},
                "tags": ["patient"],
                "params": ["patient_id"]
            },
            "患者: 我的警报状态 (表格)": {
                "sql": """
                    SELECT a.alerttype, a.timestamp, a.alertstatus, 
                           a.location, c.name AS caregiver_name
                    FROM {S}.alert a
                    JOIN {S}.healthrecord hr ON a.recordid = hr.recordid
                    JOIN {S}.wearabledevice wd ON hr.deviceid = wd.deviceid
                    LEFT JOIN {S}.caregiver c ON a.recipientcaregiverid = c.caregiverid
                    WHERE wd.assignedpatientid = :patient_id
                    ORDER BY a.timestamp DESC;
                """,
                "chart": {"type": "table"},
                "tags": ["patient"],
                "params": ["patient_id"]
            },
            "患者: 我的设备电池状态 (饼图)": {
                "sql": """
                    SELECT 
                        CASE 
                            WHEN batterylevel >= 80 THEN '80-100%'
                            WHEN batterylevel >= 60 THEN '60-79%'
                            WHEN batterylevel >= 40 THEN '40-59%'
                            WHEN batterylevel >= 20 THEN '20-39%'
                            ELSE '<20%'
                        END AS battery_range,
                        COUNT(*) AS device_count
                    FROM {S}.wearabledevice
                    WHERE assignedpatientid = :patient_id AND isactive = true
                    GROUP BY battery_range
                    ORDER BY battery_range;
                """,
                "chart": {"type": "pie", "names": "battery_range", "values": "device_count"},
                "tags": ["patient"],
                "params": ["patient_id"]
            },

            # 护理人员角色查询
            "护理人员: 需要响应的警报 (表格)": {
                "sql": """
                    SELECT p.name AS patient_name, a.alerttype, a.timestamp, 
                           a.location, a.alertstatus
                    FROM {S}.alert a
                    JOIN {S}.healthrecord hr ON a.recordid = hr.recordid
                    JOIN {S}.wearabledevice wd ON hr.deviceid = wd.deviceid
                    JOIN {S}.patient p ON wd.assignedpatientid = p.patientid
                    WHERE a.recipientcaregiverid = :caregiver_id
                      AND a.alertstatus = 'Unread'
                    ORDER BY a.timestamp DESC;
                """,
                "chart": {"type": "table"},
                "tags": ["caregiver"],
                "params": ["caregiver_id"]
            },
            "护理人员: 我负责的患者活动统计 (柱状图)": {
                "sql": """
                    SELECT p.name AS patient_name, 
                           AVG(hr.stepcount)::numeric(10,1) AS avg_steps
                    FROM {S}.patient p
                    JOIN {S}.patientcaregiverjunction pcj ON p.patientid = pcj.patientid
                    JOIN {S}.wearabledevice wd ON p.patientid = wd.assignedpatientid
                    JOIN {S}.healthrecord hr ON wd.deviceid = hr.deviceid
                    WHERE pcj.caregiverid = :caregiver_id
                      AND hr.timestamp >= CURRENT_DATE - INTERVAL '7 days'
                    GROUP BY p.name
                    ORDER BY avg_steps DESC;
                """,
                "chart": {"type": "bar", "x": "patient_name", "y": "avg_steps"},
                "tags": ["caregiver"],
                "params": ["caregiver_id"]
            },
            "护理人员: 今日警报响应统计 (表格)": {
                "sql": """
                    SELECT COUNT(*) AS total_alerts,
                           COUNT(CASE WHEN alertstatus = 'Responded' THEN 1 END) AS responded_alerts,
                           COUNT(CASE WHEN alertstatus = 'Unread' THEN 1 END) AS unread_alerts
                    FROM {S}.alert a
                    WHERE a.recipientcaregiverid = :caregiver_id
                      AND a.timestamp::date = CURRENT_DATE;
                """,
                "chart": {"type": "table"},
                "tags": ["caregiver"],
                "params": ["caregiver_id"]
            },

            # 医护人员角色查询
            "医护人员: 异常健康报告 (表格)": {
                "sql": """
                    SELECT p.name AS patient_name, hr.heartrate, hr.bloodpressure, 
                           hr.spo2, hr.timestamp, ht.metrictype, ht.minvalue, ht.maxvalue
                    FROM {S}.healthrecord hr
                    JOIN {S}.wearabledevice wd ON hr.deviceid = wd.deviceid
                    JOIN {S}.patient p ON wd.assignedpatientid = p.patientid
                    JOIN {S}.healththreshold ht ON p.patientid = ht.patientid
                    WHERE ht.staffid = :staff_id
                      AND (
                        (ht.metrictype = 'HeartRate' AND hr.heartrate NOT BETWEEN ht.minvalue AND ht.maxvalue) OR
                        (ht.metrictype = 'BloodPressure_Systolic' AND CAST(SPLIT_PART(hr.bloodpressure, '/', 1) AS INTEGER) NOT BETWEEN ht.minvalue AND ht.maxvalue) OR
                        (ht.metrictype = 'SpO2' AND hr.spo2 NOT BETWEEN ht.minvalue AND ht.maxvalue)
                      )
                    ORDER BY hr.timestamp DESC;
                """,
                "chart": {"type": "table"},
                "tags": ["medical_staff"],
                "params": ["staff_id"]
            },
            "医护人员: 患者健康阈值设置 (表格)": {
                "sql": """
                    SELECT p.name AS patient_name, ht.metrictype, ht.minvalue, ht.maxvalue
                    FROM {S}.healththreshold ht
                    JOIN {S}.patient p ON ht.patientid = p.patientid
                    WHERE ht.staffid = :staff_id
                    ORDER BY p.name, ht.metrictype;
                """,
                "chart": {"type": "table"},
                "tags": ["medical_staff"],
                "params": ["staff_id"]
            },
            "医护人员: 高风险患者统计 (柱状图)": {
                "sql": """
                    SELECT p.name AS patient_name, COUNT(a.alertid) AS alert_count
                    FROM {S}.patient p
                    JOIN {S}.wearabledevice wd ON p.patientid = wd.assignedpatientid
                    JOIN {S}.healthrecord hr ON wd.deviceid = hr.deviceid
                    JOIN {S}.alert a ON hr.recordid = a.recordid
                    WHERE a.timestamp >= CURRENT_DATE - INTERVAL '30 days'
                    GROUP BY p.name
                    HAVING COUNT(a.alertid) > :alert_threshold
                    ORDER BY alert_count DESC;
                """,
                "chart": {"type": "bar", "x": "patient_name", "y": "alert_count"},
                "tags": ["medical_staff"],
                "params": ["staff_id", "alert_threshold"]
            },

            # 系统管理员角色查询
            "管理员: 设备管理概览 (表格)": {
                "sql": """
                    SELECT wd.deviceid, wd.devicetype, wd.batterylevel, 
                           wd.isactive, p.name AS patient_name, wd.lastsynctime
                    FROM {S}.wearabledevice wd
                    LEFT JOIN {S}.patient p ON wd.assignedpatientid = p.patientid
                    ORDER BY wd.deviceid;
                """,
                "chart": {"type": "table"},
                "tags": ["admin"]
            },
            "管理员: 设备电池状态分布 (饼图)": {
                "sql": """
                    SELECT 
                        CASE 
                            WHEN batterylevel >= 80 THEN '80-100%'
                            WHEN batterylevel >= 60 THEN '60-79%'
                            WHEN batterylevel >= 40 THEN '40-59%'
                            WHEN batterylevel >= 20 THEN '20-39%'
                            ELSE '<20%'
                        END AS battery_range,
                        COUNT(*) AS device_count
                    FROM {S}.wearabledevice
                    WHERE isactive = true
                    GROUP BY battery_range
                    ORDER BY battery_range;
                """,
                "chart": {"type": "pie", "names": "battery_range", "values": "device_count"},
                "tags": ["admin"]
            },
            "管理员: 警报类型统计 (柱状图)": {
                "sql": """
                    SELECT alerttype, COUNT(*) AS alert_count
                    FROM {S}.alert
                    WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
                    GROUP BY alerttype
                    ORDER BY alert_count DESC;
                """,
                "chart": {"type": "bar", "x": "alerttype", "y": "alert_count"},
                "tags": ["admin"]
            },
            "管理员: 患者总数和平均年龄 (表格)": {
                "sql": """
                    SELECT COUNT(*) AS total_patients, 
                           AVG(age)::numeric(10,1) AS avg_age,
                           COUNT(CASE WHEN age >= 80 THEN 1 END) AS elderly_patients
                    FROM {S}.patient;
                """,
                "chart": {"type": "table"},
                "tags": ["admin"]
            },
            "管理员: 护理人员工作负载 (柱状图)": {
                "sql": """
                    SELECT c.name AS caregiver_name, 
                           COUNT(pcj.patientid) AS patient_count
                    FROM {S}.caregiver c
                    LEFT JOIN {S}.patientcaregiverjunction pcj ON c.caregiverid = pcj.caregiverid
                    GROUP BY c.name
                    ORDER BY patient_count DESC;
                """,
                "chart": {"type": "bar", "x": "caregiver_name", "y": "patient_count"},
                "tags": ["admin"]
            }
        }
    },

    "mongo": {
        "enabled": True,
        "uri": os.getenv("MONGO_URI", "mongodb://localhost:27017"),  # Will read from the .env file
        "db_name": os.getenv("MONGO_DB", "eldercare"),               # Will read from the .env file
        
        # CHANGE: Just like above, replace all the following Mongo queries with your own, for the different users you identified
        "queries": {
            # 传感器数据时序查询
            "传感器: 患者心率趋势 (过去24小时)": {
                "collection": "sensor_readings",
                "aggregate": [
                    {"$match": {
                        "patient_id": ":patient_id",
                        "timestamp": {"$gte": dt.datetime.utcnow() - dt.timedelta(hours=24)},
                        "heart_rate": {"$exists": True}
                    }},
                    {"$project": {
                        "hour": {"$dateTrunc": {"date": "$timestamp", "unit": "hour"}},
                        "heart_rate": 1
                    }},
                    {"$group": {"_id": "$hour", "avg_heart_rate": {"$avg": "$heart_rate"}, "count": {"$count": {}}}},
                    {"$sort": {"_id": 1}}
                ],
                "chart": {"type": "line", "x": "_id", "y": "avg_heart_rate"},
                "tags": ["patient", "medical_staff"]
            },
            "传感器: 血氧异常检测 (过去7天)": {
                "collection": "sensor_readings",
                "aggregate": [
                    {"$match": {
                        "timestamp": {"$gte": dt.datetime.utcnow() - dt.timedelta(days=7)},
                        "spo2": {"$lt": 92}
                    }},
                    {"$group": {"_id": "$patient_id", "low_spo2_count": {"$count": {}}}},
                    {"$sort": {"low_spo2_count": -1}}
                ],
                "chart": {"type": "bar", "x": "_id", "y": "low_spo2_count"},
                "tags": ["medical_staff", "admin"]
            },
            "传感器: 血压分布统计": {
                "collection": "sensor_readings",
                "aggregate": [
                    {"$match": {
                        "blood_pressure": {"$exists": True},
                        "timestamp": {"$gte": dt.datetime.utcnow() - dt.timedelta(days=30)}
                    }},
                    {"$addFields": {
                        "systolic": {"$toInt": {"$arrayElemAt": [{"$split": ["$blood_pressure", "/"]}, 0]}}
                    }},
                    {"$bucket": {
                        "groupBy": "$systolic",
                        "boundaries": [0, 90, 120, 140, 160, 200],
                        "default": "200+",
                        "output": {"count": {"$sum": 1}}
                    }}
                ],
                "chart": {"type": "pie", "names": "_id", "values": "count"},
                "tags": ["medical_staff", "admin"]
            },

            # 设备状态查询
            "设备: 最新设备状态 (表格)": {
                "collection": "device_status",
                "aggregate": [
                    {"$sort": {"timestamp": -1}},
                    {"$group": {"_id": "$sensor_id", "latest_status": {"$first": "$$ROOT"}}},
                    {"$replaceRoot": {"newRoot": "$latest_status"}},
                    {"$project": {
                        "_id": 0, "sensor_id": 1, "patient_id": 1, "timestamp": 1,
                        "battery_level": 1, "is_active": 1
                    }}
                ],
                "chart": {"type": "table"},
                "tags": ["admin", "caregiver"]
            },
            "设备: 电池电量分布": {
                "collection": "device_status",
                "aggregate": [
                    {"$project": {
                        "battery_level": {"$ifNull": ["$battery_level", 0]},
                        "battery_range": {
                            "$switch": {
                                "branches": [
                                    {"case": {"$gte": ["$battery_level", 80]}, "then": "80-100%"},
                                    {"case": {"$gte": ["$battery_level", 60]}, "then": "60-79%"},
                                    {"case": {"$gte": ["$battery_level", 40]}, "then": "40-59%"},
                                    {"case": {"$gte": ["$battery_level", 20]}, "then": "20-39%"},
                                ],
                                "default": "<20%"
                            }
                        }
                    }},
                    {"$group": {"_id": "$battery_range", "count": {"$count": {}}}},
                    {"$sort": {"count": -1}}
                ],
                "chart": {"type": "pie", "names": "_id", "values": "count"},
                "tags": ["admin", "caregiver"]
            },
            "设备: 低电量设备警报": {
                "collection": "device_status",
                "aggregate": [
                    {"$match": {
                        "battery_level": {"$lt": 20},
                        "is_active": True
                    }},
                    {"$group": {"_id": "$sensor_id", "low_battery_count": {"$count": {}}}},
                    {"$sort": {"low_battery_count": -1}}
                ],
                "chart": {"type": "bar", "x": "_id", "y": "low_battery_count"},
                "tags": ["admin"]
            },

            # 警报数据查询
            "警报: 最近警报统计 (表格)": {
                "collection": "alerts",
                "aggregate": [
                    {"$match": {
                        "timestamp": {"$gte": dt.datetime.utcnow() - dt.timedelta(days=7)}
                    }},
                    {"$group": {
                        "_id": "$alert_type",
                        "count": {"$count": {}},
                        "latest_timestamp": {"$max": "$timestamp"}
                    }},
                    {"$sort": {"count": -1}}
                ],
                "chart": {"type": "table"},
                "tags": ["admin", "caregiver", "medical_staff"]
            },
            "警报: 警报严重程度分布": {
                "collection": "alerts",
                "aggregate": [
                    {"$match": {
                        "timestamp": {"$gte": dt.datetime.utcnow() - dt.timedelta(days=30)}
                    }},
                    {"$group": {"_id": "$severity", "count": {"$count": {}}}},
                    {"$sort": {"count": -1}}
                ],
                "chart": {"type": "pie", "names": "_id", "values": "count"},
                "tags": ["admin", "medical_staff"]
            },
            "警报: 患者警报频率排行": {
                "collection": "alerts",
                "aggregate": [
                    {"$match": {
                        "timestamp": {"$gte": dt.datetime.utcnow() - dt.timedelta(days=30)}
                    }},
                    {"$group": {"_id": "$patient_id", "alert_count": {"$count": {}}}},
                    {"$sort": {"alert_count": -1}},
                    {"$limit": 10}
                ],
                "chart": {"type": "bar", "x": "_id", "y": "alert_count"},
                "tags": ["admin", "medical_staff"]
            },

            # 审计日志查询
            "审计: 用户操作日志 (最近24小时)": {
                "collection": "audit_logs",
                "aggregate": [
                    {"$match": {
                        "timestamp": {"$gte": dt.datetime.utcnow() - dt.timedelta(hours=24)}
                    }},
                    {"$group": {
                        "_id": "$action",
                        "count": {"$count": {}},
                        "users": {"$addToSet": "$user_id"}
                    }},
                    {"$project": {
                        "action": "$_id",
                        "count": 1,
                        "unique_users": {"$size": "$users"}
                    }},
                    {"$sort": {"count": -1}}
                ],
                "chart": {"type": "table"},
                "tags": ["admin"]
            },
            "审计: 操作类型分布": {
                "collection": "audit_logs",
                "aggregate": [
                    {"$match": {
                        "timestamp": {"$gte": dt.datetime.utcnow() - dt.timedelta(days=7)}
                    }},
                    {"$group": {"_id": "$action", "count": {"$count": {}}}},
                    {"$sort": {"count": -1}}
                ],
                "chart": {"type": "pie", "names": "_id", "values": "count"},
                "tags": ["admin"]
            },
            "审计: 用户活动趋势 (过去7天)": {
                "collection": "audit_logs",
                "aggregate": [
                    {"$match": {
                        "timestamp": {"$gte": dt.datetime.utcnow() - dt.timedelta(days=7)}
                    }},
                    {"$project": {
                        "day": {"$dateTrunc": {"date": "$timestamp", "unit": "day"}},
                        "user_id": 1
                    }},
                    {"$group": {
                        "_id": "$day",
                        "unique_users": {"$addToSet": "$user_id"},
                        "total_actions": {"$count": {}}
                    }},
                    {"$project": {
                        "day": "$_id",
                        "unique_users_count": {"$size": "$unique_users"},
                        "total_actions": 1
                    }},
                    {"$sort": {"day": 1}}
                ],
                "chart": {"type": "line", "x": "day", "y": "total_actions"},
                "tags": ["admin"]
            }
        }
    }
}

# The following block of code will create a simple Streamlit dashboard page
st.set_page_config(page_title="智能养老院健康监控系统", layout="wide")
st.title("智能养老院健康监控系统 | 数据仪表板 (Postgres + MongoDB)")

def metric_row(metrics: dict):
    cols = st.columns(len(metrics))
    for (k, v), c in zip(metrics.items(), cols):
        c.metric(k, v)

@st.cache_resource
def get_pg_engine(uri: str):
    return create_engine(uri, pool_pre_ping=True, future=True)

@st.cache_data(ttl=60)
def run_pg_query(_engine, sql: str, params: dict | None = None):
    with _engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

@st.cache_resource
def get_mongo_client(uri: str):
    return MongoClient(uri)

def mongo_overview(client: MongoClient, db_name: str):
    info = client.server_info()
    db = client[db_name]
    colls = db.list_collection_names()
    stats = db.command("dbstats")
    total_docs = sum(db[c].estimated_document_count() for c in colls) if colls else 0
    return {
        "DB": db_name,
        "Collections": f"{len(colls):,}",
        "Total docs (est.)": f"{total_docs:,}",
        "Storage": f"{round(stats.get('storageSize',0)/1024/1024,1)} MB",
        "Version": info.get("version", "unknown")
    }

@st.cache_data(ttl=60)
def run_mongo_aggregate(_client, db_name: str, coll: str, stages: list, params: dict = None):
    # 动态替换聚合管道中的参数
    if params:
        stages_str = str(stages)
        for key, value in params.items():
            stages_str = stages_str.replace(f":{key}", str(value))
        stages = eval(stages_str)
    
    db = _client[db_name]
    docs = list(db[coll].aggregate(stages, allowDiskUse=True))
    return pd.json_normalize(docs) if docs else pd.DataFrame()

def render_chart(df: pd.DataFrame, spec: dict):
    if df.empty:
        st.info("No rows.")
        return
    ctype = spec.get("type", "table")
    # light datetime parsing for x axes
    for c in df.columns:
        if df[c].dtype == "object":
            try:
                df[c] = pd.to_datetime(df[c])
            except Exception:
                pass

    if ctype == "table":
        st.dataframe(df, use_container_width=True)
    elif ctype == "line":
        st.plotly_chart(px.line(df, x=spec["x"], y=spec["y"]), use_container_width=True)
    elif ctype == "bar":
        st.plotly_chart(px.bar(df, x=spec["x"], y=spec["y"]), use_container_width=True)
    elif ctype == "pie":
        st.plotly_chart(px.pie(df, names=spec["names"], values=spec["values"]), use_container_width=True)
    elif ctype == "heatmap":
        pivot = pd.pivot_table(df, index=spec["rows"], columns=spec["cols"], values=spec["values"], aggfunc="mean")
        st.plotly_chart(px.imshow(pivot, aspect="auto", origin="upper",
                                  labels=dict(x=spec["cols"], y=spec["rows"], color=spec["values"])),
                        use_container_width=True)
    elif ctype == "treemap":
        st.plotly_chart(px.treemap(df, path=spec["path"], values=spec["values"]), use_container_width=True)
    else:
        st.dataframe(df, use_container_width=True)

# The following block of code is for the dashboard sidebar, where you can pick your users, provide parameters, etc.
with st.sidebar:
    st.header("Connections")
    # These fields are pre-filled from .env file
    pg_uri = st.text_input("Postgres URI", CONFIG["postgres"]["uri"])     
    mongo_uri = st.text_input("Mongo URI", CONFIG["mongo"]["uri"])        
    mongo_db = st.text_input("Mongo DB name", CONFIG["mongo"]["db_name"]) 
    st.divider()
    auto_run = st.checkbox("Auto-run on selection change", value=False, key="auto_run_global")

    st.header("角色与参数")
    # 智能养老院健康监控系统角色和参数
    role = st.selectbox("用户角色", ["patient", "caregiver", "medical_staff", "admin", "all"], index=4)
    
    # 患者相关参数
    patient_id = st.number_input("患者ID", min_value=1, value=1, step=1)
    
    # 护理人员相关参数
    caregiver_id = st.number_input("护理人员ID", min_value=1, value=1, step=1)
    
    # 医护人员相关参数
    staff_id = st.number_input("医护人员ID", min_value=1, value=1, step=1)
    
    # 设备相关参数
    device_id = st.number_input("设备ID", min_value=1, value=1, step=1)
    
    # 警报阈值参数
    alert_threshold = st.number_input("警报阈值", min_value=0, value=5, step=1)
    
    # 时间范围参数
    days = st.slider("过去N天", 1, 90, 7)
    
    # 电池阈值参数
    battery_threshold = st.number_input("电池低电量阈值(%)", min_value=0, max_value=100, value=20, step=5)

    PARAMS_CTX = {
        "patient_id": int(patient_id),
        "caregiver_id": int(caregiver_id),
        "staff_id": int(staff_id),
        "device_id": int(device_id),
        "alert_threshold": int(alert_threshold),
        "days": int(days),
        "battery_threshold": int(battery_threshold),
    }

#Postgres part of the dashboard
st.subheader("Postgres")
try:
    
    eng = get_pg_engine(pg_uri)

    with st.expander("Run Postgres query", expanded=True):
        # The following will filter queries by role
        def filter_queries_by_role(qdict: dict, role: str) -> dict:
            def ok(tags):
                t = [s.lower() for s in (tags or ["all"])]
                return "all" in t or role.lower() in t
            return {name: q for name, q in qdict.items() if ok(q.get("tags"))}

        pg_all = CONFIG["postgres"]["queries"]
        pg_q = filter_queries_by_role(pg_all, role)

        names = list(pg_q.keys()) or ["(no queries for this role)"]
        sel = st.selectbox("Choose a saved query", names, key="pg_sel")

        if sel in pg_q:
            q = pg_q[sel]
            sql = qualify(q["sql"])   
            st.code(sql, language="sql")

            run  = auto_run or st.button("▶ Run Postgres", key="pg_run")
            if run:
                wanted = q.get("params", [])
                params = {k: PARAMS_CTX[k] for k in wanted}
                df = run_pg_query(eng, sql, params=params)
                render_chart(df, q["chart"])
        else:
            st.info("No Postgres queries tagged for this role.")
except Exception as e:
    st.error(f"Postgres error: {e}")

# Mongo panel
if CONFIG["mongo"]["enabled"]:
    st.subheader("🍃 MongoDB")
    try:
        mongo_client = get_mongo_client(mongo_uri)   
        metric_row(mongo_overview(mongo_client, mongo_db))

        with st.expander("Run Mongo aggregation", expanded=True):
            mongo_query_names = list(CONFIG["mongo"]["queries"].keys())
            selm = st.selectbox("Choose a saved aggregation", mongo_query_names, key="mongo_sel")
            q = CONFIG["mongo"]["queries"][selm]
            st.write(f"**Collection:** `{q['collection']}`")
            st.code(str(q["aggregate"]), language="python")
            runm = auto_run or st.button("▶ Run Mongo", key="mongo_run")
            if runm:
                # 获取MongoDB查询需要的参数
                mongo_params = {}
                if "patient_id" in q.get("aggregate", []):
                    mongo_params["patient_id"] = PARAMS_CTX["patient_id"]
                if "caregiver_id" in q.get("aggregate", []):
                    mongo_params["caregiver_id"] = PARAMS_CTX["caregiver_id"]
                if "staff_id" in q.get("aggregate", []):
                    mongo_params["staff_id"] = PARAMS_CTX["staff_id"]
                if "device_id" in q.get("aggregate", []):
                    mongo_params["device_id"] = PARAMS_CTX["device_id"]
                if "alert_threshold" in q.get("aggregate", []):
                    mongo_params["alert_threshold"] = PARAMS_CTX["alert_threshold"]
                if "battery_threshold" in q.get("aggregate", []):
                    mongo_params["battery_threshold"] = PARAMS_CTX["battery_threshold"]
                
                dfm = run_mongo_aggregate(mongo_client, mongo_db, q["collection"], q["aggregate"], mongo_params)
                render_chart(dfm, q["chart"])
    except Exception as e:
        st.error(f"Mongo error: {e}")
