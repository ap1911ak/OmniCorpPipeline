import re
import datetime
import pandas as pd
import numpy as np

from .monadsquishy import validator, transformer
# from monadsquishy.monadsquishy import validator, transformer

# =========================
# Constants & Regex Patterns
# =========================
TH_PREFIX_PATTERN = re.compile(
    r'(หสน\.|หสม\.|หจก\.|บจก\.|บจม\.|บมจ\.|บจ\.|หจ\.|หส\.|บ\.|ห้างหุ้นส่วนสามัญ|ห้างหุ้นส่วน|ห้างหุ้นส่วนจำกัด)'
)

TH_POSTFIX_PATTERN = re.compile(
    r'(จำกัด \(มหาชน\)|จำกัด จำกัด|จำกัด|จก\.)'
)

EN_POSTFIX_PATTERN = re.compile(
    r'(CO\., LTD\.|CO\.,LTD\.|CO\.,TLD\.|CO, LTD\.|CO\., LTD|CO\.,LTD|'
    r'LTD\., PART\.|LTD\.,PART\.|LTD\., PART|LTD\.,PART|'
    r'PART\.|LTD\.|'
    r'COMPANY LIMITED|COMPANY LIMTED|CORPORATION LIMITED|'
    r'ORDINARY PARTNERSHIP|LIMITED PARTNERSHIP|LIMITED PARNERSHIP|LIMITED)',
    re.IGNORECASE,
)


COMPANY_STATUS = {
    "ยังดำเนินกิจการอยู่",
    "พิทักษ์ทรัพย์เด็ดขาด",
    "แปรสภาพ",
    "เลิก",
    "เสร็จการชำระบัญชี",
    "ควบ",
    "ร้าง",
    "ล้มละลาย",
    "ฟื้นฟู",
    "คืนสู่ทะเบียน",
    "สิ้นสภาพ",
}

COUNTRY_ALIAS_MAP = {
    # Short name → Full official name
    'UNITED KINGDOM'                    : 'UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND',
    'UNITED STATES'                     : 'UNITED STATES OF AMERICA',
    'NETHERLANDS'                       : 'NETHERLANDS, KINGDOM OF THE',
    'CZECH REPUBLIC'                    : 'CZECHIA',
    'VIETNAM'                           : 'VIET NAM',
    'IRAN'                              : 'IRAN, ISLAMIC REPUBLIC OF',
    'BOLIVIA'                           : 'BOLIVIA, PLURINATIONAL STATE OF',
    'VENEZUELA'                         : 'VENEZUELA, BOLIVARIAN REPUBLIC OF',
    'TANZANIA'                          : 'TANZANIA, UNITED REPUBLIC OF',
    'MOLDOVA, REPUBLIC OF'              : 'MOLDOVA, REPUBLIC OF',
    "LAO PEOPLE'S DEMOCRATIC REPUBLIC"  : "LAO PEOPLE'S DEMOCRATIC REPUBLIC",
    'DEMOCRATIC REPUBLIC OF THE CONGO'  : 'CONGO, DEMOCRATIC REPUBLIC OF THE',
    'LIBYAN ARAB JAMAHIRIYA'            : 'LIBYA',
    'CAPE VERDE'                        : 'CABO VERDE',

    # SAR / Special territories
    'HONG KONG SAR'                     : 'HONG KONG',
    'MACAO SAR'                         : 'MACAO',
    'TAIWAN'                            : 'TAIWAN, PROVINCE OF CHINA',

    # Diacritics / special chars stripped in source
    'TURKIYE'                           : 'TÜRKİYE',
    'CURACAO'                           : 'CURAÇAO',
    "COTE D'IVOIRE"                     : "CÔTE D'IVOIRE",
    'REUNION'                           : 'RÉUNION',

    # Partial name in source
    'SINT MAARTEN'                      : 'SINT MAARTEN (DUTCH PART)',
    'KOREA, REPUBLIC OF'                : 'KOREA, REPUBLIC OF',

    # Not in dim_area — no ISO mapping available
    'KOSOVO'                            : None,
}


# =========================
# Generic Validators
# =========================
@validator
def must_exist(x):
    if (
        pd.isna(x)
        or str(x).strip() == ''
        or str(x).strip().lower() in ['n/a', 'null', 'nan', '-', 'ไม่ระบุ']
    ):
        raise Exception("Missing")
    return x


@validator
def validate_tax_no(x):
    pattern1 = r"^0[1-9][0-9][2-9][4-5][0-9][0-9]\d{0,5}[0-9]$"
    pattern1_1 = r"^0[1-9][0-9][2-9]00[0-9]\d{0,5}[0-9]$"
    pattern2 = r"^099[2-9][4-5][0-9][0-9]\d{0,5}[0-9]$"
    pattern2_2 = r"^099[2-9]00[0-9]\d{0,5}[0-9]$"
    pattern3 = r"^0[1-9][0-9]0[4-5][0-9][0-9]\d{0,5}[0-9]$"

    def checksum_ok(num: str) -> bool:
        weights = [13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2]
        total = sum(int(num[i]) * weights[i] for i in range(12))
        remainder = total % 11
        check_digit = (11 - remainder) % 10
        return check_digit == int(num[12])

    if x.isdigit() and x.startswith("0") and len(x) == 13:
        # First group of patterns
        if (
            (
                re.match(pattern2, x)
                or re.match(pattern1, x)
                or re.match(pattern2_2, x)
                or re.match(pattern1_1, x)
            )
            and re.match(r'^\d{3}(?!0)\d', x)
        ):
            if 421 <= int(x[4:7]) <= 570 or x[4:7] == '000':
                if checksum_ok(x):
                    return x
        # Second pattern group
        elif re.match(pattern3, x) and re.match(r'^\d{3}0\d', x):
            if 421 <= int(x[4:7]) <= 570:
                if checksum_ok(x):
                    return x

    # If any condition is not satisfied
    raise Exception("Invalid Tax No")


@validator
def validate_thai_citizen_id(x):
    total = sum(int(x[i]) * (13 - i) for i in range(12))
    remainder = total % 11
    calculated_check_digit = (11 - remainder) % 10
    # Compare calculated check digit with the provided 13th digit
    if calculated_check_digit == int(x[12]):
        return x
    raise Exception("Invalid citizen_id")


@validator
def validate_name_length(x) -> str:
    """(Validator) Fails if name length < 3 chars."""
    s_value = str(x).strip()
    if len(s_value) < 3:
        raise Exception(f"Name '{s_value}' is less than 3 characters.")
    return s_value

@validator
def validate_no_test_company(x) -> str:
    forbidden = {'บจ.ทดสอบ'}
    s = str(x).strip().lower()
    if any(word in s for word in forbidden):  # detect partial match
        raise Exception(f"Name '{x}' contains forbidden word.")
    return str(x).strip()

@validator
def validate_no_test_description(x) -> str:
    forbidden = {'test topthai'}
    s = str(x).strip().lower()
    if any(word in s for word in forbidden):  # detect partial match
        raise Exception(f"Name '{x}' contains forbidden word.")
    return str(x).strip()

@validator
def validate_no_test(x) -> str:
    forbidden = {'ทดสอบระบบ', 'test test', 'บจ.ทดสอบ', 'test', 'เทส', 'ทดสอบ'}
    s = str(x).strip().lower()
    if any(word in s for word in forbidden):  # detect partial match
        raise Exception(f"Name '{x}' contains forbidden word.")
    return str(x).strip()

@validator
def validate_map_compnany_status(x):
    status_map = {
        "1": "ยังดำเนินกิจการอยู่",
        "2": "พิทักษ์ทรัพย์เด็ดขาด",
        "3": "แปรสภาพ",
        "4": "เลิก",
        "5": "เสร็จการชำระบัญชี",
        "6": "ควบ",
        "8": "ร้าง",
        "9": "ล้มละลาย",
        "A": "ฟื้นฟู",
        "B": "คืนสู่ทะเบียน",
        "D": "สิ้นสภาพ",
        # None: "ไม่ระบุ"
    }
    if str(x) in status_map:
        return status_map.get(str(x), None)
    raise Exception(f"'{x}' is not in the company status.")


@validator
def validate_company_status(x):
    x = str(x).strip()
    if x in COMPANY_STATUS:
        return x
    raise ValueError(f"'{x}' is not a valid company status.")
    
@validator
def validate_post_code(x) -> str:
    s = str(x).strip()
    pattern = r"^[1-9][0-9]{4}$"
    if not re.fullmatch(pattern, s):
        raise Exception(f"Invalid Thai post code: '{x}'")
    return s

@validator
def validate_hscode_id(x):
    s = str(x).strip()
    if len(s) == 15 and s.isdigit():
        return x

@validator
def validate_hscode_length(x):
    val = str(x).strip()
    length = len(val)
    allowed_lengths = [2, 4, 6, 8, 11]
    if length in allowed_lengths:
        return val
    raise Exception(f"Invalid HS Code length: '{val}' has {length} digits. Allowed lengths are {allowed_lengths}.")

@validator
def validate_year(x):
    year = int(x)
    if year > 1970 and year < datetime.datetime.now().year + 10:
        return str(x)
    raise Exception(f"'{x}' is out of validate year.")


@validator
def validate_month(x):
    month = int(x)
    if 1 <= month <= 12:
        return str(x)
    raise Exception(f"'{x}' is out of month range.")


@validator
def validate_participation_type(x):
    if x in ('Corporate', 'Person'):
        return x
    raise Exception(f"'{x}' is not in participation_type.")


@validator
def validate_ditp_member(x):
    if x in [
        "EL",
        "Pre-EL",
        "TDC",
        "Pre-TDC",
        "SPL",
        "SEL",
        "LSP",
        "Pre-SMEX",
        "SMEX",
        "Non Member",
    ]:
        return x
    raise Exception(f"'{x}' is not in ditp member.")


@validator
def validate_email(x):
    s = str(x).strip().lower()

    # Simple email regex pattern
    pattern = r"[^@\s]+@[^@\s]+\.[^@\s]+"

    # Extract all email-like patterns
    emails = re.findall(pattern, s)

    if not emails:
        raise Exception(f"No valid email found in: '{x}'")

    # Take the first email found
    email = emails[0]

    # Final strict validation using fullmatch
    if not re.fullmatch(pattern, email):
        raise Exception(f"Invalid email: '{email}'")

    return email


@validator
def validate_th_phone(x):
    raw = str(x)
    s = re.sub(r'\D', '', raw)

    if s.startswith("66") and len(s) >= 9:
        s = "0" + s[2:]

    if not s.isdigit():
        raise Exception(f"Phone must be digits only: '{x}'")
    if len(s) not in (9, 10):
        raise Exception(f"Phone length must be 9 or 10 digits: '{x}'")
    if not s.startswith("0"):
        raise Exception(f"Phone must start with 0: '{x}'")

    return s


@validator
def validate_person_type(x):
    if x in ["บุคคลไทย", "บุคคลต่างชาติ", "นิติบุคคลไทย", "นิติบุคคลต่างชาติ"]:
        return x
    raise Exception(f"'{x}' is not in validate preson_type")


# =========================
# Transformers
# =========================
@transformer
def normalize_whitespace(x):
    """Normalize leading, trailing, and repeated whitespace without changing NULL."""
    if pd.isna(x):
        return x
    return re.sub(r'\s+', ' ', str(x)).strip()

@transformer
def clean_string(x):
    try:
        x = str(x)
        # Replace question marks with space
        if "?" in x:
            x = x.replace("?", " ")
        # Normalize whitespace
        x = re.sub(r"\s+", " ", x)
        return x.strip()
    except Exception:
        raise Exception("Cannot clean string")

@transformer
def transform_to_lowercase(x):
    return str(x).lower().strip()

@transformer
def normalize_tax_no(tax_no):
    """
    Data Quality: ซ่อมแซมและคัดกรองเลขประจำตัวผู้เสียภาษี (Juristic ID)
    - หากเป็นค่าว่าง จะปัดเป็น None
    - หากความยาว 12 หลัก จะพยายามเติม 0 ด้านหน้า
    - หากสุดท้ายความยาวไม่ใช่ 13 หลัก หรือมีอักขระอื่นที่ไม่ใช่ตัวเลข จะปัดเป็น None
    """
    if pd.isna(tax_no) or str(tax_no).strip() in ['', 'nan', 'None']:
        return None
        
    s = str(tax_no).replace("'", "").strip()
    
    # 1. กรณีศูนย์หาย: ถ้ามี 12 หลัก ให้ลองเติม 0 ข้างหน้า 1 ตัว
    if len(s) == 12:
        s = "0" + s
        
    # 2. กรองข้อมูลขยะ: ถ้าสุดท้ายแล้วความยาวไม่ใช่ 13 หลัก ให้คืนค่า None (ค่าว่าง) ทันที
    if len(s) != 13:
        return None
        
    # 3. ตรวจสอบเพิ่มเติม: บังคับว่าต้องเป็นตัวเลขทั้งหมดเท่านั้น
    if not s.isdigit():
        return None
        
    return s

@transformer
def transform_company_name(x):
    try:
        x = str(x)
        if "?" in x:
            x = x.replace("?", " ")
            x = re.sub(r"\s+", " ", x)
            return x.strip()
        return x.strip()
    except Exception:
        raise Exception("Cannot clean name_th")
    
@transformer
def transform_country_name(x):    
    normalized = str(x).strip().upper()
    # 1. Check alias map first
    if normalized in COUNTRY_ALIAS_MAP:
        return COUNTRY_ALIAS_MAP[normalized]   # returns None for Kosovo → stays NULL
    # 2. Already matches dim key (e.g. 'CHINA', 'JAPAN')
    return normalized


@transformer
def parse_thai_date_to_greorian(x):
    """Convert 'DD/MM/YYYY' Thai year date to Gregorian datetime."""
    m = re.match(r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})', str(x).strip())
    if not m:
        raise ValueError(f"Invalid date format: {x!r}")

    def _thai_to_gregorian_year(x):
        """Convert Thai Buddhist year to Gregorian."""
        year = int(x)  # will raise if not numeric — this is intended behavior
        if year > 2400:
            return year - 543
        return year

    day, month, year_th = map(int, m.groups())
    year_greg = _thai_to_gregorian_year(year_th)
    return datetime.datetime(year_greg, month, day)


@transformer
def transform_to_float(x):
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        raise Exception("Cannot transform to float")


@transformer
def extract_postfix_th(x):
    """Extract and normalize Thai company postfix from name_th."""
    x = str(x)
    normalized_prefix = extract_prefix_th(x)

    if normalized_prefix in (
        'ห้างหุ้นส่วนจำกัด',
        'ห้างหุ้นส่วนสามัญนิติบุคคล',
        'ห้างหุ้นส่วนสามัญ',
    ):
        return None

    m = TH_POSTFIX_PATTERN.search(x)
    if not m:
        return None

    postfix_th = m.group(1)
    if postfix_th in ('จำกัด จำกัด', 'จก.'):
        return 'จำกัด'
    return postfix_th


@transformer
def extract_postfix_en(x):
    """Extract and normalize English company postfix from name_en."""
    x = str(x)
    m = EN_POSTFIX_PATTERN.search(x.upper())
    if not m:
        return None

    postfix_en = m.group(1).upper()

    if postfix_en in (
        'CO., LTD.',
        'CO.,LTD.',
        'CO.,TLD.',
        'CO, LTD.',
        'CO., LTD',
        'CO.,LTD',
    ):
        return 'COMPANY LIMITED'
    elif postfix_en == 'COMPANY LIMTED':
        return 'COMPANY LIMITED'
    elif postfix_en in (
        'LTD., PART.',
        'LTD.,PART.',
        'LTD., PART',
        'LTD.,PART',
        'LIMITED PARNERSHIP',
    ):
        return 'LIMITED PARTNERSHIP'
    elif postfix_en == 'LTD.':
        return 'LIMITED'
    elif postfix_en == 'PART.':
        return 'PARTNERSHIP'
    return postfix_en


@transformer
def extract_company_name_th(x):
    """Remove Thai prefix & postfix and return core company name."""
    x = str(x)
    x = TH_PREFIX_PATTERN.sub('', x)
    x = TH_POSTFIX_PATTERN.sub('', x)
    return re.sub(r'\s+', ' ', x).strip()


@transformer
def extract_company_name_en(x):
    """Remove English postfix & return core English company name."""
    x = str(x)
    x = EN_POSTFIX_PATTERN.sub('', x)
    return re.sub(r'\s+', ' ', x).strip()


@transformer
def transform_province_name(x):
    s = str(x).strip()

    # Normalize lowercase for comparisons
    lower_s = s.lower()

    if lower_s == "phang-nga":
        return s.replace("-", "")
    elif lower_s == "bangkok":
        return "Krung Thep Maha Nakhon"
    return s


@transformer
def transform_person_type(x):
    try:
        if x == "นิติคุคคลต่างชาติ":
            return "นิติบุคคลต่างชาติ"
        else:
            return x
    except Exception:
        raise Exception("Cannot transfrom preson_type")
    
@transformer
def transform_filepath_to_ditpcode(x):
    """
    Extract root keyword from folder name to match dim_ditp_code.
    e.g. '01_สินค้าอิเล็กทรอนิกส์' → 'อิเล็กทรอนิกส์'
         '09_สินค้าเครื่องจักรและส่วนประกอบ' → 'เครื่องจักร'  (before และ)
    """
    if x is None:
        return x
    # Strip leading digits + สินค้า prefix
    cleaned = re.sub(r'^\d+_สินค้า?', '', str(x).strip())
    # Take part before 'และ' as root keyword
    return cleaned.split('และ')[0].strip()

# =========================
# Custom Transformers for DITP Posts
# =========================
@transformer
def strip_html_tags(x):
    """ลบ HTML Tags ออกจากเนื้อหาบทความ"""
    if pd.isna(x) or not x or str(x).strip().lower() in ['nan', 'none', 'null']:
        return ""
    # ลบ HTML tags
    clean_text = re.sub(r'<[^>]*>', '', str(x))
    # จัดการช่องว่างที่เกิดจาก tag เช่น &nbsp; หรือการขึ้นบรรทัดใหม่
    clean_text = re.sub(r'\s+', ' ', clean_text)
    return clean_text.strip()

@transformer
def transform_to_int(x):
    """แปลงค่าตัวเลขประเภท float/string ให้เป็น Integer"""
    if pd.isna(x) or str(x).strip() in ['', 'nan', 'None']:
        return 0
    try:
        return int(float(x))
    except (ValueError, TypeError):
        return 0

# rules.py

def clean_string(column_name):
    """ลบช่องว่างส่วนเกินและตัดขอบข้อความ"""
    return f"TRIM(REGEXP_REPLACE(CAST({column_name} AS VARCHAR), '\\s+', ' ', 'g'))"

def strip_html_tags(column_name):
    """ลบ HTML Tags ออกจากเนื้อหา"""
    return f"TRIM(REGEXP_REPLACE(CAST({column_name} AS VARCHAR), '<[^>]*>', '', 'g'))"

def transform_to_int(column_name):
    """พยายามแปลงเป็นตัวเลข ถ้าพังจะคืนค่า NULL"""
    return f"TRY_CAST({column_name} AS INTEGER)"

def transform_to_datetime(column_name):
    """แปลงเป็นวันที่ (Timestamp)"""
    return f"TRY_CAST({column_name} AS TIMESTAMP)"
