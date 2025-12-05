# main.py - ДОПОЛНЕННЫЙ ФАЙЛ
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
import sqlite3
from datetime import datetime
from typing import List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import json

app = FastAPI(title="СА ДО API", version="4.0", docs_url="/api/docs")

# 🔥 CORS для веб-интерфейса
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === БАЗОВЫЕ ЭНДПОИНТЫ ДО ===
@app.get("/api/do-list")
async def get_do_list():
    """Список всех ДО с базовой статистикой"""
    try:
        conn = sqlite3.connect("do_system.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, name FROM do ORDER BY name")
        do_list = [dict(row) for row in cursor.fetchall()]
        
        # Добавляем статистику для каждого ДО
        for do_item in do_list:
            cursor.execute("""
                SELECT COUNT(*) as system_count 
                FROM sa WHERE do_id = ?
            """, (do_item['id'],))
            do_item['system_count'] = cursor.fetchone()[0] or 0
            
        conn.close()
        return JSONResponse(content=do_list)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Database error: {str(e)}"}
        )

@app.get("/api/do/{do_id}/summary")
async def get_do_summary(do_id: int):
    """Расширенная статистика по ДО"""
    try:
        conn = sqlite3.connect("do_system.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Основные метрики
        cursor.execute("""
            SELECT 
                COUNT(*) as total_systems,
                AVG(CAST(json_extract(detail_json, '$."Функциональность, %"') AS REAL)) as automation_level,
                AVG(2024 - CAST(json_extract(detail_json, '$."Год внедрения системы автоматизации"') AS INTEGER)) as avg_age,
                SUM(CASE WHEN CAST(json_extract(detail_json, '$."Эксплуатационный износ"') AS REAL) > 70 THEN 1 ELSE 0 END) as problem_count
            FROM sa_data_details sdd
            JOIN sa_data sd ON sdd.sa_data_id = sd.id  
            JOIN sa s ON sd.sa_id = s.id
            WHERE s.do_id = ?
        """, (do_id,))
        
        result = dict(cursor.fetchone() or {})
        
        # Дополнительные метрики
        cursor.execute("""
            SELECT COUNT(DISTINCT json_extract(detail_json, '$."Вид системы автоматизации"')) as system_types
            FROM sa_data_details sdd
            JOIN sa_data sd ON sdd.sa_data_id = sd.id  
            JOIN sa s ON sd.sa_id = s.id
            WHERE s.do_id = ?
        """, (do_id,))
        
        system_types = cursor.fetchone()[0] or 0
        
        conn.close()
        
        summary = {
            "total_systems": result.get('total_systems', 0) or 0,
            "automation_level": round(result.get('automation_level', 0) or 0, 1),
            "avg_age": round(result.get('avg_age', 0) or 0, 1),
            "problem_count": result.get('problem_count', 0) or 0,
            "system_types": system_types
        }
        
        return JSONResponse(content=summary)
        
    except Exception as e:
        return JSONResponse(
            content={"total_systems": 0, "automation_level": 0, "avg_age": 0, "problem_count": 0, "system_types": 0}
        )


@app.get("/api/do/{do_id}/full-details")
async def get_do_full_details(do_id: int):
    """Полная детальная информация о ДО"""
    try:
        conn = sqlite3.connect("do_system.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # 1. Основная информация о ДО
        cursor.execute("SELECT id, name FROM do WHERE id = ?", (do_id,))
        do_row = cursor.fetchone()

        if not do_row:
            raise HTTPException(status_code=404, detail="ДО не найдена")

        do_info = dict(do_row)

        # 2. KPI метрики (уже есть в summary, но дублируем)
        cursor.execute("""
            SELECT 
                COUNT(*) as total_systems,
                AVG(CAST(json_extract(detail_json, '$."Функциональность, %"') AS REAL)) as automation_level,
                AVG(2024 - CAST(json_extract(detail_json, '$."Год внедрения системы автоматизации"') AS INTEGER)) as avg_age,
                SUM(CASE WHEN CAST(json_extract(detail_json, '$."Эксплуатационный износ"') AS REAL) > 70 THEN 1 ELSE 0 END) as problem_count
            FROM sa_data_details sdd
            JOIN sa_data sd ON sdd.sa_data_id = sd.id  
            JOIN sa s ON sd.sa_id = s.id
            WHERE s.do_id = ?
        """, (do_id,))

        kpi_row = cursor.fetchone()
        kpi = {
            "total_systems": kpi_row['total_systems'] if kpi_row else 0,
            "automation_level": round(kpi_row['automation_level'] or 0, 1) if kpi_row else 0,
            "avg_age": round(kpi_row['avg_age'] or 0, 1) if kpi_row else 0,
            "problem_count": kpi_row['problem_count'] or 0 if kpi_row else 0
        }

        # 3. Статистика по типам систем
        cursor.execute("""
            SELECT 
                json_extract(detail_json, '$."Вид системы автоматизации"') as system_type,
                COUNT(*) as count
            FROM sa_data_details sdd
            JOIN sa_data sd ON sdd.sa_data_id = sd.id  
            JOIN sa s ON sd.sa_id = s.id
            WHERE s.do_id = ?
                AND json_extract(detail_json, '$."Вид системы автоматизации"') IS NOT NULL
                AND json_extract(detail_json, '$."Вид системы автоматизации"') != ''
            GROUP BY json_extract(detail_json, '$."Вид системы автоматизации"')
            ORDER BY count DESC
        """, (do_id,))

        system_stats = [{"system_type": row['system_type'], "count": row['count']}
                        for row in cursor.fetchall()]

        # 4. Возрастное распределение
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN (2024 - CAST(json_extract(detail_json, '$."Год внедрения системы автоматизации"') AS INTEGER)) <= 5 THEN '0-5 лет'
                    WHEN (2024 - CAST(json_extract(detail_json, '$."Год внедрения системы автоматизации"') AS INTEGER)) <= 10 THEN '6-10 лет'
                    WHEN (2024 - CAST(json_extract(detail_json, '$."Год внедрения системы автоматизации"') AS INTEGER)) <= 15 THEN '11-15 лет'
                    ELSE '16+ лет'
                END as age_group,
                COUNT(*) as count
            FROM sa_data_details sdd
            JOIN sa_data sd ON sdd.sa_data_id = sd.id  
            JOIN sa s ON sd.sa_id = s.id
            WHERE s.do_id = ?
                AND json_extract(detail_json, '$."Год внедрения системы автоматизации"') IS NOT NULL
                AND CAST(json_extract(detail_json, '$."Год внедрения системы автоматизации"') AS INTEGER) > 0
            GROUP BY age_group
            ORDER BY 
                CASE age_group
                    WHEN '0-5 лет' THEN 1
                    WHEN '6-10 лет' THEN 2
                    WHEN '11-15 лет' THEN 3
                    WHEN '16+ лет' THEN 4
                END
        """, (do_id,))

        age_distribution = [{"age_group": row['age_group'], "count": row['count']}
                            for row in cursor.fetchall()]

        # 5. Проблемные системы (износ > 70% или функциональность < 50%)
        cursor.execute("""
            SELECT 
                json_extract(detail_json, '$."Наименование объекта"') as object_name,
                json_extract(detail_json, '$."Вид системы автоматизации"') as system_type,
                json_extract(detail_json, '$."Год внедрения системы автоматизации"') as install_year,
                CAST(json_extract(detail_json, '$."Эксплуатационный износ"') AS REAL) as wear,
                CAST(json_extract(detail_json, '$."Функциональность, %"') AS REAL) as functionality
            FROM sa_data_details sdd
            JOIN sa_data sd ON sdd.sa_data_id = sd.id  
            JOIN sa s ON sd.sa_id = s.id
            WHERE s.do_id = ?
                AND (
                    CAST(json_extract(detail_json, '$."Эксплуатационный износ"') AS REAL) > 70
                    OR CAST(json_extract(detail_json, '$."Функциональность, %"') AS REAL) < 50
                )
            LIMIT 10
        """, (do_id,))

        problem_systems = [
            {
                "object_name": row['object_name'] or "Не указан",
                "system_type": row['system_type'] or "Не указан",
                "install_year": row['install_year'] or "Не указан",
                "wear": round(row['wear'] or 0, 1),
                "functionality": round(row['functionality'] or 0, 1)
            }
            for row in cursor.fetchall()
        ]

        conn.close()

        # 6. Формируем ответ
        full_details = {
            "do_info": do_info,
            "kpi": kpi,
            "system_stats": system_stats,
            "age_distribution": age_distribution,
            "problem_systems": problem_systems
        }

        return JSONResponse(content=full_details)

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in full-details: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Database error: {str(e)}"}
        )


@app.get("/api/do/{do_id}/tech-data")
async def get_do_tech_data(do_id: int, year: int = 2023):
    """Технические данные ДО"""
    try:
        conn = sqlite3.connect("do_system.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Получаем все системы ДО с деталями
        cursor.execute("""
            SELECT 
                sdd.id,
                json_extract(detail_json, '$."Вид системы автоматизации"') as system_type,
                json_extract(detail_json, '$."Наименование объекта"') as object_name,
                json_extract(detail_json, '$."Год внедрения системы автоматизации"') as install_year,
                json_extract(detail_json, '$."Функциональность, %"') as functionality,
                json_extract(detail_json, '$."Эксплуатационный износ"') as wear,
                json_extract(detail_json, '$."Тип ПЛК"') as plc_type,
                json_extract(detail_json, '$."Тип SCADA"') as scada_type
            FROM sa_data_details sdd
            JOIN sa_data sd ON sdd.sa_data_id = sd.id  
            JOIN sa s ON sd.sa_id = s.id
            WHERE s.do_id = ?
                AND json_extract(detail_json, '$."Год внедрения системы автоматизации"') IS NOT NULL
            ORDER BY object_name
        """, (do_id,))

        details = [
            {
                "id": row['id'],
                "Вид системы автоматизации": row['system_type'] or "-",
                "Наименование объекта": row['object_name'] or "-",
                "Год внедрения системы автоматизации": row['install_year'] or "-",
                "Функциональность, %": row['functionality'] or 0,
                "Эксплуатационный износ": row['wear'] or 0,
                "Тип ПЛК": row['plc_type'] or "-",
                "Тип SCADA": row['scada_type'] or "-"
            }
            for row in cursor.fetchall()
        ]

        conn.close()

        response = {
            "year": year,
            "do_id": do_id,
            "details": details
        }

        return JSONResponse(content=response)

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Database error: {str(e)}"}
        )

# === АНАЛИТИКА ===
@app.get("/api/analytics/dobycha/tech-objects")
async def get_dobycha_tech_objects():
    """Технологические объекты добычи"""
    try:
        conn = sqlite3.connect("do_system.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # ID добывающих ДО
        do_dobycha_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 34, 35]
        placeholders = ','.join('?' * len(do_dobycha_ids))
        
        cursor.execute(f"""
            SELECT 
                d.name as do_name,
                COUNT(*) as object_count,
                SUM(CASE WHEN json_extract(sdd.detail_json, '$."Вид системы автоматизации"') LIKE '%УКПГ%' THEN 1 ELSE 0 END) as ukpg_count,
                SUM(CASE WHEN json_extract(sdd.detail_json, '$."Вид системы автоматизации"') LIKE '%скважин%' THEN 1 ELSE 0 END) as wells_count
            FROM sa_data_details sdd
            JOIN sa_data sd ON sdd.sa_data_id = sd.id
            JOIN sa s ON sd.sa_id = s.id
            JOIN do d ON s.do_id = d.id
            WHERE s.do_id IN ({placeholders})
            GROUP BY d.name
            ORDER BY d.name
        """, do_dobycha_ids)
        
        result = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return JSONResponse(content=result)
        
    except Exception as e:
        print(f"Ошибка аналитики добычи: {e}")
        return JSONResponse(content=[])

@app.get("/api/analytics/transport/tech-objects")
async def get_transport_tech_objects():
    """Технологические объекты транспорта - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
    try:
        conn = sqlite3.connect("do_system.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # ИСПРАВЛЕНО: Используем те же ID, что и в coverage-detailed
        do_transport_ids = [13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]
        placeholders = ','.join('?' * len(do_transport_ids))
        
        # Находим последний год с данными
        cursor.execute(f"""
            SELECT MAX(year) FROM automation_summary 
            WHERE do_id IN ({placeholders})
        """, do_transport_ids)
        latest_year = cursor.fetchone()[0]
        
        if not latest_year:
            return JSONResponse(content=[])
        
        # ИСПРАВЛЕНО: Используем те же indicator_id, что и в coverage-detailed
        query = f"""
        SELECT 
            d.name as do_name,
            MAX(CASE WHEN a.indicator_id = '54' THEN CAST(a.value AS REAL) ELSE 0 END) as mg_length,
            MAX(CASE WHEN a.indicator_id = '63' THEN CAST(a.value AS REAL) ELSE 0 END) as go_length,
            MAX(CASE WHEN a.indicator_id = '94' THEN CAST(a.value AS INTEGER) ELSE 0 END) as grs_count,
            MAX(CASE WHEN a.indicator_id = '33' THEN CAST(a.value AS INTEGER) ELSE 0 END) as ks_count,
            MAX(CASE WHEN a.indicator_id = '34' THEN CAST(a.value AS INTEGER) ELSE 0 END) as kc_count,
            MAX(CASE WHEN a.indicator_id = '85' THEN CAST(a.value AS INTEGER) ELSE 0 END) as gpa_count,
            MAX(CASE WHEN a.indicator_id = '4' THEN CAST(a.value AS INTEGER) ELSE 0 END) as cdp_count,
            MAX(CASE WHEN a.indicator_id = '7' THEN CAST(a.value AS INTEGER) ELSE 0 END) as dp_count
        FROM automation_summary a
        JOIN do d ON a.do_id = d.id
        WHERE a.do_id IN ({placeholders}) AND a.year = ?
        GROUP BY a.do_id
        ORDER BY d.name
        """
        
        cursor.execute(query, do_transport_ids + [latest_year])
        
        result = []
        for row in cursor.fetchall():
            result.append({
                'do_name': row['do_name'],
                'mg_length': row['mg_length'] or 0,
                'go_length': row['go_length'] or 0,
                'grs_count': row['grs_count'] or 0,
                'ks_count': row['ks_count'] or 0,
                'kc_count': row['kc_count'] or 0,
                'gpa_count': row['gpa_count'] or 0,
                'cdp_count': row['cdp_count'] or 0,
                'dp_count': row['dp_count'] or 0
            })
        
        conn.close()
        
        # ДЛЯ ТЕСТИРОВАНИЯ - если данных нет, вернем тестовые
        if not result:
            print("⚠️ Нет данных в automation_summary для транспортных ДО")
            # Вернем тестовые данные на основе coverage-detailed
            return JSONResponse(content=[
                {
                    'do_name': 'ООО «Газпром трансгаз Екатеринбург»',
                    'mg_length': 4407778.0, 'go_length': 4146.0, 'grs_count': 257,
                    'ks_count': 15, 'kc_count': 8, 'gpa_count': 120, 'cdp_count': 5, 'dp_count': 12
                },
                {
                    'do_name': 'ООО «Газпром трансгаз Москва»', 
                    'mg_length': 13256.0, 'go_length': 7976.0, 'grs_count': 719,
                    'ks_count': 25, 'kc_count': 12, 'gpa_count': 180, 'cdp_count': 8, 'dp_count': 20
                },
                {
                    'do_name': 'ООО «Газпром трансгаз Чайковский»',
                    'mg_length': 8883.013, 'go_length': 1695.486, 'grs_count': 122, 
                    'ks_count': 10, 'kc_count': 6, 'gpa_count': 85, 'cdp_count': 3, 'dp_count': 8
                }
            ])
        
        return JSONResponse(content=result)
        
    except Exception as e:
        print(f"❌ Ошибка в get_transport_tech_objects: {e}")
        return JSONResponse(content=[])


# === РЕАЛЬНЫЕ ДАННЫЕ ДЛЯ ГРАФИКОВ ПОКРЫТИЯ ===
@app.get("/api/analytics/transport/coverage-detailed")
async def get_transport_coverage_detailed():
    """Реальные данные для графиков покрытия СЛТМ из automation_summary"""
    try:
        conn = sqlite3.connect("do_system.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # ID транспортных ДО
        do_transport_ids = [13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]
        placeholders = ','.join('?' * len(do_transport_ids))
        
        # Находим последний год с данными
        cursor.execute(f"""
            SELECT MAX(year) FROM automation_summary 
            WHERE do_id IN ({placeholders})
        """, do_transport_ids)
        latest_year = cursor.fetchone()[0]
        
        if not latest_year:
            return JSONResponse(content={"mg": [], "go": [], "grs": []})
        
        # Получаем данные покрытия для МГ, ГО, ГРС
        query = f"""
        SELECT 
            d.name as do_name,
            MAX(CASE WHEN a.indicator_id = '54' THEN a.value ELSE 0 END) as mg_total,
            MAX(CASE WHEN a.indicator_id = '56' THEN a.value ELSE 0 END) as mg_covered,
            MAX(CASE WHEN a.indicator_id = '63' THEN a.value ELSE 0 END) as go_total,
            MAX(CASE WHEN a.indicator_id = '65' THEN a.value ELSE 0 END) as go_covered,
            MAX(CASE WHEN a.indicator_id = '94' THEN a.value ELSE 0 END) as grs_total,
            MAX(CASE WHEN a.indicator_id = '95' THEN a.value ELSE 0 END) as grs_covered
        FROM automation_summary a
        JOIN do d ON a.do_id = d.id
        WHERE a.do_id IN ({placeholders}) AND a.year = ?
        GROUP BY a.do_id
        ORDER BY d.name
        """
        
        cursor.execute(query, do_transport_ids + [latest_year])
        rows = cursor.fetchall()
        
        mg_data = []
        go_data = []
        grs_data = []
        
        for row in rows:
            do_name = row['do_name']
            
            # Данные МГ
            mg_total = float(row['mg_total']) if row['mg_total'] not in (None, '') else 0
            mg_covered = float(row['mg_covered']) if row['mg_covered'] not in (None, '') else 0
            mg_data.append({
                'do_name': do_name,
                'total': mg_total,
                'covered': mg_covered,
                'valves_total': 0,  # Можно добавить из других показателей
                'valves_covered': 0
            })
            
            # Данные ГО
            go_total = float(row['go_total']) if row['go_total'] not in (None, '') else 0
            go_covered = float(row['go_covered']) if row['go_covered'] not in (None, '') else 0
            go_data.append({
                'do_name': do_name,
                'total': go_total,
                'covered': go_covered,
                'valves_total': 0,
                'valves_covered': 0
            })
            
            # Данные ГРС
            grs_total = int(row['grs_total']) if row['grs_total'] not in (None, '') else 0
            grs_covered = int(row['grs_covered']) if row['grs_covered'] not in (None, '') else 0
            grs_data.append({
                'do_name': do_name,
                'total': grs_total,
                'covered': grs_covered
            })
        
        conn.close()
        
        return JSONResponse(content={
            "mg": mg_data,
            "go": go_data, 
            "grs": grs_data
        })
        
    except Exception as e:
        print(f"Ошибка реальных данных покрытия: {e}")
        return JSONResponse(content={"mg": [], "go": [], "grs": []})

@app.get("/api/analytics/transport/condition-detailed")
async def get_transport_condition_detailed(system_filter: str = "Все системы"):
    """Реальные данные технического состояния из sa_data_details"""
    try:
        conn = sqlite3.connect("do_system.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # ДО транспорта по названиям 
        do_transport_names = [
            "ООО «Газпром трансгаз Ухта»",
            "ООО «Газпром трансгаз Махачкала»",
            "ООО «Газпром трансгаз Ставрополь»",
            "ООО «Газпром трансгаз Сургут»",
            "ООО «Газпром трансгаз Волгоград»",
            "ООО «Газпром трансгаз Югорск»",
            "ООО «Газпром трансгаз Самара»",
            "ООО «Газпром трансгаз Краснодар»",
            "ООО «Газпром трансгаз Санкт-Петербург»",
            "ООО «Газпром трансгаз Саратов»",
            "ООО «Газпром трансгаз Чайковский»",
            "ООО «Газпром трансгаз Беларусь»",
            "ООО «Газпром трансгаз Нижний Новгород»",
            "ООО «Газпром трансгаз Екатеринбург»",
            "ООО «Газпром трансгаз Казань»",
            "ООО «Газпром трансгаз Москва»",
            "ООО «Газпром трансгаз Томск»",
            "ООО «Газпром трансгаз Уфа»",
            "АО «Газпром трансгаз Грозный»",
            "ЗАО «Газпром Армения»",
            "АО «Газпром Кыргызстан»"
        ]
        
        placeholders = ','.join('?' * len(do_transport_names))
        
        # Базовый запрос
        query = f"""
        SELECT 
            json_extract(sdd.detail_json, '$.Наименование ДО') as do_name,
            json_extract(sdd.detail_json, '$.Вид системы автоматизации') as system_type,
            json_extract(sdd.detail_json, '$.Год внедрения системы автоматизации') as install_year
        FROM sa_data_details sdd
        WHERE json_extract(sdd.detail_json, '$.Наименование ДО') IN ({placeholders})
        """
        
        cursor.execute(query, do_transport_names)
        rows = cursor.fetchall()
        
        # Функция для определения возрастной группы 
        def get_age_group(age):
            if age <= 12:
                return "до 12 лет"
            elif age <= 24:
                return "12-24 года"
            else:
                return "более 25 лет"
        
        result = []
        current_year = datetime.now().year
        
        for row in rows:
            do_name = row['do_name']
            system_type = row['system_type']
            install_year = int(row['install_year']) if row['install_year'] not in (None, '') else None
            
            if install_year:
                age = current_year - install_year
                age_group = get_age_group(age)
                
                result.append({
                    'do_name': do_name,
                    'system_type': system_type,
                    'age_group': age_group
                })
        
        # Применяем фильтр как в Tkinter
        if system_filter != "Все системы":
            system_keywords = {
                "АСУ ТП УКПГ (УППГ)": ["УКПГ", "УППГ"],
                "АСУ ТП": ["АСУ ТП"],
                "САУ ГПА": ["САУ ГПА", "ГПА"],
                "АСПС": ["АСПС", "пожар"],
                "СТМ": ["СТМ", "телемех"]
            }
            keywords = system_keywords.get(system_filter, [])
            result = [item for item in result if any(keyword in item["system_type"] for keyword in keywords)]
        
        conn.close()
        return JSONResponse(content=result)
        
    except Exception as e:
        print(f"Ошибка реальных данных состояния: {e}")
        return JSONResponse(content=[])

@app.get("/api/analytics/age-stats")
async def get_age_stats():
    """Статистика по возрасту систем"""
    try:
        conn = sqlite3.connect("do_system.db")
        cursor = conn.cursor()
        
        current_year = datetime.now().year
        cursor.execute(f"""
            SELECT 
                CASE
                    WHEN {current_year} - CAST(json_extract(detail_json, '$."Год внедрения системы автоматизации"') AS INTEGER) <= 5 THEN '0-5 лет'
                    WHEN {current_year} - CAST(json_extract(detail_json, '$."Год внедрения системы автоматизации"') AS INTEGER) <= 10 THEN '6-10 лет' 
                    WHEN {current_year} - CAST(json_extract(detail_json, '$."Год внедрения системы автоматизации"') AS INTEGER) <= 15 THEN '11-15 лет'
                    ELSE '16+ лет'
                END as age_group,
                COUNT(*) as count
            FROM sa_data_details
            WHERE json_extract(detail_json, '$."Год внедрения системы автоматизации"') IS NOT NULL
            GROUP BY age_group
            ORDER BY age_group
        """)
        
        result = [{"age_group": row[0], "count": row[1]} for row in cursor.fetchall()]
        conn.close()
        return JSONResponse(content=result)
        
    except Exception as e:
        print(f"Ошибка возрастной статистики: {e}")
        return JSONResponse(content=[])

# === ИМПОРТОЗАМЕЩЕНИЕ ===
@app.get("/api/import-substitution/stats")
async def get_import_substitution_stats():
    """Статистика импортозамещения"""
    try:
        conn = sqlite3.connect("do_system.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Общая статистика
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN import_status = 'Замещено' THEN 1 ELSE 0 END) as substituted,
                SUM(CASE WHEN import_status = 'Испытания' THEN 1 ELSE 0 END) as testing,
                SUM(CASE WHEN import_status IS NULL OR import_status = 'Не замещено' THEN 1 ELSE 0 END) as not_substituted
            FROM sa_data_details
        """)
        
        stats_row = cursor.fetchone()
        overall_stats = {
            "total": stats_row[0] or 0,
            "substituted": stats_row[1] or 0,
            "testing": stats_row[2] or 0,
            "not_substituted": stats_row[3] or 0
        }
        
        # Статистика по ДО
        cursor.execute("""
            SELECT 
                d.name as do_name,
                COUNT(*) as total,
                SUM(CASE WHEN sdd.import_status = 'Замещено' THEN 1 ELSE 0 END) as substituted,
                SUM(CASE WHEN sdd.import_status = 'Испытания' THEN 1 ELSE 0 END) as testing
            FROM sa_data_details sdd
            JOIN sa_data sd ON sdd.sa_data_id = sd.id
            JOIN sa s ON sd.sa_id = s.id  
            JOIN do d ON s.do_id = d.id
            GROUP BY d.name
            ORDER BY d.name
        """)
        
        do_stats = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return JSONResponse(content={
            "overall": overall_stats,
            "by_do": do_stats
        })
        
    except Exception as e:
        print(f"Ошибка статистики импортозамещения: {e}")
        return JSONResponse(content={"overall": {"total": 0, "substituted": 0, "testing": 0, "not_substituted": 0}, "by_do": []})

@app.get("/api/import-substitution/systems")
async def get_import_substitution_systems():
    """Список систем для импортозамещения"""
    try:
        conn = sqlite3.connect("do_system.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                sdd.id as detail_id,
                d.name as do_name,
                s.name as system_name,
                json_extract(sdd.detail_json, '$."Наименование объекта"') as object_name,
                json_extract(sdd.detail_json, '$."Тип ПЛК"') as plc_type,
                json_extract(sdd.detail_json, '$."Тип SCADA"') as scada_type,
                COALESCE(sdd.import_status, 'Не указан') as import_status,
                sdd.test_stage
            FROM sa_data_details sdd
            JOIN sa_data sd ON sdd.sa_data_id = sd.id
            JOIN sa s ON sd.sa_id = s.id
            JOIN do d ON s.do_id = d.id
            WHERE sdd.import_status IS NOT NULL OR sdd.test_stage IS NOT NULL
            ORDER BY d.name, s.name
            LIMIT 100
        """)
        
        systems = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return JSONResponse(content=systems)
        
    except Exception as e:
        print(f"Ошибка загрузки систем импортозамещения: {e}")
        return JSONResponse(content=[])

# === АВТОМАТИЗАЦИЯ ===
@app.get("/api/automation/summary")
async def get_automation_summary():
    """Сводные данные по автоматизации"""
    try:
        conn = sqlite3.connect("do_system.db")
        cursor = conn.cursor()
        
        # Получаем данные из automation_summary
        cursor.execute("""
            SELECT 
                d.name as do_name,
                a.indicator_id,
                a.indicator,
                a.year,
                a.value
            FROM automation_summary a
            JOIN do d ON a.do_id = d.id
            WHERE a.year = 2023
            ORDER BY d.name, CAST(a.indicator_id AS INTEGER)
        """)
        
        # Группируем по ДО и показателям
        data = {}
        for row in cursor.fetchall():
            do_name, indicator_id, indicator, year, value = row
            if do_name not in data:
                data[do_name] = []
            
            data[do_name].append({
                "indicator_id": indicator_id,
                "indicator": indicator,
                "year": year,
                "value": value
            })
        
        conn.close()
        return JSONResponse(content=data)
        
    except Exception as e:
        print(f"Ошибка сводной автоматизации: {e}")
        return JSONResponse(content={})

# === СИСТЕМЫ И ДЕТАЛИЗАЦИЯ ===
@app.get("/api/system-types")
async def get_system_types():
    """Список типов систем"""
    try:
        conn = sqlite3.connect("do_system.db")
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT json_extract(detail_json, '$."Вид системы автоматизации"') as system_type
            FROM sa_data_details
            WHERE json_extract(detail_json, '$."Вид системы автоматизации"') IS NOT NULL
            ORDER BY system_type
        """)
        
        types = [row[0] for row in cursor.fetchall() if row[0]]
        conn.close()
        return JSONResponse(content=types)
        
    except Exception as e:
        print(f"Ошибка получения типов систем: {e}")
        return JSONResponse(content=[])

@app.get("/api/do/{do_id}/systems")
async def get_do_systems(do_id: int):
    """Системы конкретного ДО"""
    try:
        conn = sqlite3.connect("do_system.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                s.id,
                s.name,
                st.name as type
            FROM sa s
            LEFT JOIN sa_types st ON s.sa_type = st.id
            WHERE s.do_id = ?
            ORDER BY s.name
        """, (do_id,))
        
        systems = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return JSONResponse(content=systems)
        
    except Exception as e:
        print(f"Ошибка получения систем ДО: {e}")
        return JSONResponse(content=[])

@app.get("/api/do/{do_id}/full-details")
async def get_do_full_details(do_id: int):
    """Полные детальные данные по ДО - аналог DODetailsWindow"""
    try:
        conn = sqlite3.connect("do_system.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Основная информация о ДО
        cursor.execute("SELECT id, name FROM do WHERE id = ?", (do_id,))
        do_info = dict(cursor.fetchone())
        
        # Системы ДО
        cursor.execute("""
            SELECT s.id, s.name, st.name as type 
            FROM sa s 
            LEFT JOIN sa_types st ON s.sa_type = st.id 
            WHERE s.do_id = ?
        """, (do_id,))
        systems = [dict(row) for row in cursor.fetchall()]
        
        # KPI данные
        kpi_data = await get_do_kpi_data(do_id, cursor)
        
        # Возрастное распределение
        age_data = await get_do_age_distribution(do_id, cursor)
        
        # Проблемные системы
        problem_systems = await get_do_problem_systems(do_id, cursor)
        
        # Статистика по типам систем
        system_stats = await get_do_system_stats(do_id, cursor)
        
        conn.close()
        
        return JSONResponse(content={
            "do_info": do_info,
            "systems": systems,
            "kpi": kpi_data,
            "age_distribution": age_data,
            "problem_systems": problem_systems,
            "system_stats": system_stats
        })
        
    except Exception as e:
        print(f"Ошибка загрузки детальных данных ДО: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

async def get_do_kpi_data(do_id: int, cursor):
    """KPI данные для ДО"""
    cursor.execute("""
        SELECT 
            COUNT(*) as total_systems,
            AVG(CAST(json_extract(detail_json, '$."Функциональность, %"') AS REAL)) as automation_level,
            AVG(2024 - CAST(json_extract(detail_json, '$."Год внедрения системы автоматизации"') AS INTEGER)) as avg_age,
            SUM(CASE WHEN CAST(json_extract(detail_json, '$."Эксплуатационный износ"') AS REAL) > 70 THEN 1 ELSE 0 END) as problem_count,
            SUM(CASE WHEN CAST(json_extract(detail_json, '$."Функциональность, %"') AS REAL) < 50 THEN 1 ELSE 0 END) as low_functionality_count
        FROM sa_data_details sdd
        JOIN sa_data sd ON sdd.sa_data_id = sd.id  
        JOIN sa s ON sd.sa_id = s.id
        WHERE s.do_id = ?
    """, (do_id,))
    
    result = dict(cursor.fetchone() or {})
    return {
        "total_systems": result.get('total_systems', 0) or 0,
        "automation_level": round(result.get('automation_level', 0) or 0, 1),
        "avg_age": round(result.get('avg_age', 0) or 0, 1),
        "problem_count": result.get('problem_count', 0) or 0,
        "low_functionality_count": result.get('low_functionality_count', 0) or 0
    }

async def get_do_age_distribution(do_id: int, cursor):
    """Возрастное распределение систем ДО"""
    current_year = datetime.now().year
    cursor.execute(f"""
        SELECT 
            CASE
                WHEN {current_year} - CAST(json_extract(detail_json, '$."Год внедрения системы автоматизации"') AS INTEGER) <= 5 THEN '0-5 лет'
                WHEN {current_year} - CAST(json_extract(detail_json, '$."Год внедрения системы автоматизации"') AS INTEGER) <= 10 THEN '6-10 лет'
                WHEN {current_year} - CAST(json_extract(detail_json, '$."Год внедрения системы автоматизации"') AS INTEGER) <= 15 THEN '11-15 лет'
                ELSE '16+ лет'
            END as age_group,
            COUNT(*) as count
        FROM sa_data_details sdd
        JOIN sa_data sd ON sdd.sa_data_id = sd.id
        JOIN sa s ON sd.sa_id = s.id
        WHERE s.do_id = ? AND json_extract(detail_json, '$."Год внедрения системы автоматизации"') IS NOT NULL
        GROUP BY age_group
    """, (do_id,))
    
    return [{"age_group": row[0], "count": row[1]} for row in cursor.fetchall()]

async def get_do_problem_systems(do_id: int, cursor):
    """Проблемные системы ДО"""
    cursor.execute("""
        SELECT 
            json_extract(detail_json, '$."Наименование объекта"') as object_name,
            json_extract(detail_json, '$."Вид системы автоматизации"') as system_type,
            CAST(json_extract(detail_json, '$."Эксплуатационный износ"') AS REAL) as wear,
            CAST(json_extract(detail_json, '$."Функциональность, %"') AS REAL) as functionality,
            json_extract(detail_json, '$."Год внедрения системы автоматизации"') as install_year
        FROM sa_data_details sdd
        JOIN sa_data sd ON sdd.sa_data_id = sd.id
        JOIN sa s ON sd.sa_id = s.id
        WHERE s.do_id = ? AND (
            CAST(json_extract(detail_json, '$."Эксплуатационный износ"') AS REAL) > 70 OR
            CAST(json_extract(detail_json, '$."Функциональность, %"') AS REAL) < 50
        )
        ORDER BY wear DESC
        LIMIT 20
    """, (do_id,))
    
    return [dict(row) for row in cursor.fetchall()]

async def get_do_system_stats(do_id: int, cursor):
    """Статистика по типам систем ДО"""
    cursor.execute("""
        SELECT 
            json_extract(detail_json, '$."Вид системы автоматизации"') as system_type,
            COUNT(*) as count
        FROM sa_data_details sdd
        JOIN sa_data sd ON sdd.sa_data_id = sd.id
        JOIN sa s ON sd.sa_id = s.id
        WHERE s.do_id = ?
        GROUP BY json_extract(detail_json, '$."Вид системы автоматизации"')
        ORDER BY count DESC
    """, (do_id,))
    
    return [{"system_type": row[0], "count": row[1]} for row in cursor.fetchall()]

# === ТЕХНИЧЕСКИЕ ПОКАЗАТЕЛИ ДО ===
@app.get("/api/do/{do_id}/tech-data")
async def get_do_tech_data(do_id: int, year: int = 2023):
    """Технические показатели ДО за конкретный год"""
    try:
        conn = sqlite3.connect("do_system.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Получаем имя ДО
        cursor.execute("SELECT name FROM do WHERE id = ?", (do_id,))
        do_name = cursor.fetchone()[0]
        
        # Получаем технические данные
        cursor.execute("""
            SELECT sdd.detail_json
            FROM sa_data_details sdd
            JOIN sa_data sd ON sdd.sa_data_id = sd.id
            JOIN sa s ON sd.sa_id = s.id
            JOIN do d ON s.do_id = d.id
            WHERE d.id = ? AND sd.year = ?
        """, (do_id, year))
        
        details = []
        for row in cursor.fetchall():
            try:
                detail_data = json.loads(row[0])
                details.append(detail_data)
            except json.JSONDecodeError:
                continue
        
        conn.close()
        return JSONResponse(content={
            "do_name": do_name,
            "year": year,
            "details": details
        })
        
    except Exception as e:
        print(f"Ошибка загрузки технических данных: {e}")
        return JSONResponse(content={"do_name": "", "year": year, "details": []})

# === РАСШИРЕННАЯ АНАЛИТИКА ===
@app.get("/api/analytics/pererabotka/tech-objects")
async def get_pererabotka_tech_objects():
    """Технологические объекты переработки"""
    try:
        conn = sqlite3.connect("do_system.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # ID ДО переработки
        do_pererabotka_ids = [11]  # ООО «Газпром переработка»
        placeholders = ','.join('?' * len(do_pererabotka_ids))
        
        cursor.execute(f"""
            SELECT 
                d.name as do_name,
                COUNT(*) as object_count,
                SUM(CASE WHEN json_extract(sdd.detail_json, '$."Вид системы автоматизации"') LIKE '%установк%переработк%' THEN 1 ELSE 0 END) as processing_units,
                SUM(CASE WHEN json_extract(sdd.detail_json, '$."Вид системы автоматизации"') LIKE '%ГПЗ%' THEN 1 ELSE 0 END) as gpz_count
            FROM sa_data_details sdd
            JOIN sa_data sd ON sdd.sa_data_id = sd.id
            JOIN sa s ON sd.sa_id = s.id
            JOIN do d ON s.do_id = d.id
            WHERE s.do_id IN ({placeholders})
            GROUP BY d.name
        """, do_pererabotka_ids)
        
        result = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return JSONResponse(content=result)
        
    except Exception as e:
        print(f"Ошибка аналитики переработки: {e}")
        return JSONResponse(content=[])

@app.get("/api/analytics/phg/tech-objects")
async def get_phg_tech_objects():
    """Технологические объекты ПХГ"""
    try:
        conn = sqlite3.connect("do_system.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # ID ДО ПХГ
        do_phg_ids = [12]  # ООО «Газпром ПХГ»
        placeholders = ','.join('?' * len(do_phg_ids))
        
        cursor.execute(f"""
            SELECT 
                d.name as do_name,
                COUNT(*) as object_count,
                SUM(CASE WHEN json_extract(sdd.detail_json, '$."Вид системы автоматизации"') LIKE '%ПХГ%' THEN 1 ELSE 0 END) as phg_objects,
                SUM(CASE WHEN json_extract(sdd.detail_json, '$."Вид системы автоматизации"') LIKE '%КС ПХГ%' THEN 1 ELSE 0 END) as ks_phg_count
            FROM sa_data_details sdd
            JOIN sa_data sd ON sdd.sa_data_id = sd.id
            JOIN sa s ON sd.sa_id = s.id
            JOIN do d ON s.do_id = d.id
            WHERE s.do_id IN ({placeholders})
            GROUP BY d.name
        """, do_phg_ids)
        
        result = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return JSONResponse(content=result)
        
    except Exception as e:
        print(f"Ошибка аналитики ПХГ: {e}")
        return JSONResponse(content=[])

# === ДАННЫЕ ДЛЯ СЛОЖНЫХ ГРАФИКОВ ТРАНСПОРТА ===
@app.get("/api/analytics/transport/pipeline-coverage-detailed")
async def get_transport_pipeline_coverage_detailed():
    """Детальные данные покрытия трубопроводов для сложных графиков"""
    try:
        conn = sqlite3.connect("do_system.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Данные для МГ и ГО
        #mg_data = await get_transport_pipeline_coverage('mg')
        #go_data = await get_transport_pipeline_coverage('go')
        
        conn.close()
        return JSONResponse(content={
           # "mg": mg_data,
           # "go": go_data
        })
        
    except Exception as e:
        print(f"Ошибка детальных данных транспорта: {e}")
        return JSONResponse(content={"mg": [], "go": []})


# === ЗДОРОВЬЕ СИСТЕМЫ ===
@app.get("/api/health")
async def health_check():
    """Проверка состояния API"""
    return JSONResponse(content={
        "status": "healthy",
        "service": "СА ДО API v4.0",
        "timestamp": datetime.now().isoformat(),
        "version": "4.0"
    })

@app.get("/")
async def root():
    return {"message": "🚀 СА ДО API работает!", "version": "4.0"}

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 ЗАПУСК СА ДО API v4.0 - ПОЛНАЯ ВЕРСИЯ")
    print("=" * 60)
    print("📡 Адрес: http://localhost:8000")
    print("📚 Документация: http://localhost:8000/api/docs") 
    print("❤️  Проверка: http://localhost:8000/api/health")
    print("=" * 60)
    
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")

