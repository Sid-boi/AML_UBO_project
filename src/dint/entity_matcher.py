# src/dint/entity_matcher.py

from thefuzz import fuzz
import jellyfish
import yaml
from typing import Dict, List, Tuple, Optional
from dateutil import parser
import logging

logger = logging.getLogger(__name__)


class EntityMatcher:
    """
    Production-grade entity matching with RIGID threshold logic.
    
    Philosophy:
    - Deterministic matching (no adaptive magic)
    - Clear decision rules (explainable to regulators)
    - Name is always present (guaranteed by contract)
    
    Matching Rules:
    1. Name + DOB match → MATCH (0.60+ score, high confidence)
    2. Name + Address match → MATCH (0.55+ score, medium confidence)
    3. Name only (0.90+ score) → MATCH (low confidence)
    4. Otherwise → NO MATCH
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path) as f:
            config = yaml.safe_load(f)
        
        self.weights = config["entity_resolution"]["weights"]
        self.threshold = config["entity_resolution"]["threshold"]  # 0.75 default
        self.penalties = config["entity_resolution"]["penalties"]
        
        logger.info(f"EntityMatcher initialized with threshold={self.threshold}")
    
    def _safe_str(self, v):
        """Safe string conversion"""
        if v is None:
            return ""
        if isinstance(v, float):
            return ""
        return str(v)
    
    def calculate_score(self, entity1: Dict, entity2: Dict) -> Tuple[float, Dict]:
        """Calculate weighted similarity score"""
        
        scores = {}
        
        # NAME (already safe - _normalize_name handles it)
        name1 = self._normalize_name(entity1.get("full_name", ""))
        name2 = self._normalize_name(entity2.get("full_name", ""))
        
        if not name1 or not name2:
            logger.error("Missing name in entity matching!")
            return 0.0, {"error": "missing_name"}
        
        name_sim = self._compare_names(name1, name2)
        scores["name"] = name_sim * self.weights["name"]
        
        # DOB (keep as-is, dates are fine)
        dob1 = entity1.get("dob")
        dob2 = entity2.get("dob")
        
        if dob1 and dob2:
            dob_sim = self._compare_dates(dob1, dob2)
            scores["dob"] = dob_sim * self.weights["dob"]
        else:
            scores["dob"] = 0.0
        
        # ADDRESS (already using _safe_str - good!)
        addr1 = self._safe_str(entity1.get("address"))
        addr2 = self._safe_str(entity2.get("address"))
        
        if addr1 and addr2:
            addr_sim = self._compare_addresses(addr1, addr2)
            scores["address"] = addr_sim * self.weights["address"]
        else:
            scores["address"] = 0.0
        
        # COMPANY NUMBER (keep as-is, handled differently)
        if entity1.get("entity_type") == "company" and entity2.get("entity_type") == "company":
            comp1 = self._safe_str(entity1.get("company_number", ""))  # ✅
            comp2 = self._safe_str(entity2.get("company_number", ""))  # ✅
            
            if comp1 and comp2:
                if comp1 == comp2:
                    scores["company_number"] = self.weights["company_number"]
                else:
                    scores["company_number"] = 0.0
            else:
                scores["company_number"] = 0.0
        else:
            scores["company_number"] = 0.0
        
        # NATIONALITY ✅ FIX
        nat1 = self._safe_str(entity1.get("nationality", ""))
        nat2 = self._safe_str(entity2.get("nationality", ""))
        
        if nat1 and nat2 and nat1.lower() == nat2.lower():
            scores["nationality"] = self.weights["nationality"]
        else:
            scores["nationality"] = 0.0
        
        # COUNTRY ✅ FIX
        country1 = self._safe_str(entity1.get("country", ""))
        country2 = self._safe_str(entity2.get("country", ""))
        
        if country1 and country2 and country1.lower() == country2.lower():
            scores["country"] = self.weights["country"]
        else:
            scores["country"] = 0.0
        
        # Total
        total = sum(scores.values())
        total = max(0.0, min(1.0, total))
        
        return total, scores
    
    def should_match(self, entity1: Dict, entity2: Dict) -> Tuple[bool, float, Dict, str]:
        """
        3-tier confidence matching with SEPARATE logic for persons vs companies.
        
        Returns:
        - should_match: bool
        - score: float
        - breakdown: dict
        - reason: str (includes confidence level)
        """
        
        score, breakdown = self.calculate_score(entity1, entity2)
        
        # Check entity types
        type1 = entity1.get('entity_type', '').lower()
        type2 = entity2.get('entity_type', '').lower()
        
        # ═══════════════════════════════════════════════════════
        # RULE 0: Different entity types → NO MATCH
        # ═══════════════════════════════════════════════════════
        if type1 != type2:
            reason = f"type_mismatch ({type1} vs {type2})"
            logger.debug(f"No match: {reason}")
            return False, score, breakdown, reason
        
        # ═══════════════════════════════════════════════════════
        # COMPANY MATCHING (Different rules!)
        # ═══════════════════════════════════════════════════════
        if type1 == 'company':
            return self._match_companies(entity1, entity2, score, breakdown)
        
        # ═══════════════════════════════════════════════════════
        # PERSON MATCHING (Your existing logic)
        # ═══════════════════════════════════════════════════════
        else:
            return self._match_persons(entity1, entity2, score, breakdown)


    def _match_companies(
    self, 
    entity1: Dict, 
    entity2: Dict, 
    score: float, 
    breakdown: Dict
) -> Tuple[bool, float, Dict, str]:
        """
        Company matching logic with MANDATORY company number for cross-source matching.
        """
        
        has_company_num = breakdown.get("company_number", 0) > 0
        has_address = breakdown.get("address", 0) > 0
        
        # Normalize name score (0-1 range)
        name_score = breakdown.get("name", 0) / self.weights["name"]
        
        # Check source types
        source1 = entity1.get('source_type', '')
        source2 = entity2.get('source_type', '')
        same_source = (source1 == source2)

        
        
        # ✅ NEW RULE: For CROSS-SOURCE matching, company_number is MANDATORY
        if not same_source and not has_company_num:
            reason = f"no_match (cross_source_requires_company_number, score={score:.3f})"
            logger.info(f"❌ Cross-source company match REJECTED (no company number): {reason}")
            return False, score, breakdown, reason
        
        # 🔥 CRITICAL RULE: Different company numbers → NO MATCH!
        comp_num1 = self._safe_str(entity1.get('company_number', ''))
        comp_num2 = self._safe_str(entity2.get('company_number', ''))

        if comp_num1 and comp_num2 and comp_num1 != comp_num2:
            reason = f"no_match (company_numbers_differ: {comp_num1} vs {comp_num2})"
            logger.info(f"❌ Company match REJECTED: {reason}")
            return False, score, breakdown, reason
        
        # Check date incorporated
        date_inc1 = entity1.get('date_incorporated')
        date_inc2 = entity2.get('date_incorporated')
        has_date_inc = bool(date_inc1 and date_inc2)
        
        if has_date_inc:
            try:
                date_inc_match = (date_inc1 == date_inc2)
            except:
                date_inc_match = False
        else:
            date_inc_match = False
        
        # ═══════════════════════════════════════════════════════
        # HIGH CONFIDENCE
        # ═══════════════════════════════════════════════════════
        
        # C-H1: Company number exact match (GOLD STANDARD!)
        if has_company_num:
            reason = f"HIGH:company_number_exact (score={score:.3f})"
            logger.info(f"HIGH confidence match (company): {reason}")
            return True, score, breakdown, reason
        
        # For SAME-SOURCE matches, allow name-based matching
        if same_source:
            # C-H2: Near-exact name + address (same source only)
            if name_score >= 0.92 and has_address and score >= 0.45:
                reason = f"HIGH:name_exact+address_same_source (score={score:.3f})"
                logger.info(f"HIGH confidence match (company, same source): {reason}")
                return True, score, breakdown, reason
            
            # C-M1: Strong name + address (same source only)
            if name_score >= 0.85 and has_address and score >= 0.38:
                reason = f"MEDIUM:strong_name+address_same_source (score={score:.3f})"
                logger.info(f"MEDIUM confidence match (company, same source): {reason}")
                return True, score, breakdown, reason
        
        # ═══════════════════════════════════════════════════════
        # NO MATCH
        # ═══════════════════════════════════════════════════════
        
        reason = f"no_match (score={score:.3f}, name={name_score:.3f})"
        logger.debug(f"No match (company): {reason}")
        return False, score, breakdown, reason
    def _match_persons(
        self, 
        entity1: Dict, 
        entity2: Dict, 
        score: float, 
        breakdown: Dict
    ) -> Tuple[bool, float, Dict, str]:
        """
        Person matching logic (your existing code).
        
        Persons have:
        - Name
        - DOB (most reliable!)
        - Address
        - Nationality
        """
        
        has_dob = breakdown.get("dob", 0) > 0
        has_address = breakdown.get("address", 0) > 0
        
        # Normalize name score (0-1 range)
        name_score = breakdown.get("name", 0) / self.weights["name"]
        
        # ═══════════════════════════════════════════════════════
        # HIGH CONFIDENCE (Person-specific)
        # ═══════════════════════════════════════════════════════
        
        # P-H1: Name + DOB (GOLD STANDARD!)
        if has_dob and score >= 0.60:
            reason = f"HIGH:name+dob (score={score:.3f})"
            logger.info(f"HIGH confidence match (person): {reason}")
            return True, score, breakdown, reason
        
        # P-H2: Near-exact name + address
        if name_score >= 0.95 and has_address and score >= 0.50:
            reason = f"HIGH:name_exact+address (score={score:.3f})"
            logger.info(f"HIGH confidence match (person): {reason}")
            return True, score, breakdown, reason
        
        # ═══════════════════════════════════════════════════════
        # MEDIUM CONFIDENCE (Person-specific)
        # ═══════════════════════════════════════════════════════
        
        # P-M1: Strong name + address
        if name_score >= 0.85 and has_address and score >= 0.40:
            reason = f"MEDIUM:strong_name+address (score={score:.3f})"
            logger.info(f"MEDIUM confidence match (person): {reason}")
            return True, score, breakdown, reason
        
        # P-M2: Strong name + partial DOB
        if has_dob and name_score >= 0.85 and score >= 0.45:
            reason = f"MEDIUM:name+dob_partial (score={score:.3f})"
            logger.info(f"MEDIUM confidence match (person): {reason}")
            return True, score, breakdown, reason
        
        # P-M3: Very strong name alone (risky but flagged)
        if name_score >= 0.92 and score >= 0.33:
            reason = f"MEDIUM:name_very_strong (score={score:.3f})"
            logger.info(f"MEDIUM confidence match (person): {reason}")
            return True, score, breakdown, reason
        
        # ═══════════════════════════════════════════════════════
        # NO MATCH
        # ═══════════════════════════════════════════════════════
        
        reason = f"no_match (score={score:.3f}, name={name_score:.3f})"
        logger.debug(f"No match (person): {reason}")
        return False, score, breakdown, reason
        
    # ═══════════════════════════════════════════════════════
    # COMPARISON METHODS (UNCHANGED)
    # ═══════════════════════════════════════════════════════
    
    def _compare_names(self, name1: str, name2: str) -> float:
        """Multi-strategy name comparison"""
        
        if not name1 or not name2:
            return 0.0
        
        # Strategy 1: Token sort (handles reordering)
        token_score = fuzz.token_sort_ratio(name1, name2) / 100.0
        
        # Strategy 2: Partial ratio (handles abbreviations)
        partial_score = fuzz.partial_ratio(name1, name2) / 100.0
        
        # Strategy 3: Abbreviation detection
        abbrev_score = self._check_abbreviation(name1, name2)
        
        # Strategy 4: Phonetic matching
        phonetic_score = 0.0
        try:
            words1 = name1.split()
            words2 = name2.split()
            
            has_abbrev = any(
                len(w.replace('.', '').replace(',', '')) <= 1 
                for w in words1 + words2
            )
            
            if not has_abbrev and len(name1) > 3 and len(name2) > 3:
                soundex_match = jellyfish.soundex(name1) == jellyfish.soundex(name2)
                metaphone_match = jellyfish.metaphone(name1) == jellyfish.metaphone(name2)
                
                if soundex_match and metaphone_match:
                    phonetic_score = 0.90
                elif soundex_match or metaphone_match:
                    phonetic_score = 0.75
        except Exception:
            pass
        
        # Take best strategy
        best_score = max(token_score, partial_score, abbrev_score, phonetic_score)
        
        logger.debug(
            f"  Name strategies: token={token_score:.2f}, partial={partial_score:.2f}, "
            f"abbrev={abbrev_score:.2f}, phonetic={phonetic_score:.2f} → best={best_score:.2f}"
        )
        
        return best_score
    
    def _check_abbreviation(self, name1: str, name2: str) -> float:
        """Check if one name is abbreviation of another"""
        
        words1 = name1.split()
        words2 = name2.split()
        
        if abs(len(words1) - len(words2)) > 1:
            return 0.0
        
        max_len = max(len(words1), len(words2))
        words1 = words1 + [''] * (max_len - len(words1))
        words2 = words2 + [''] * (max_len - len(words2))
        
        matches = 0.0
        total = 0
        
        for w1, w2 in zip(words1, words2):
            if not w1 or not w2:
                continue
            
            total += 1
            
            clean1 = w1.replace('.', '').replace(',', '').lower()
            clean2 = w2.replace('.', '').replace(',', '').lower()
            
            if clean1 == clean2:
                matches += 1.0
            elif len(clean1) == 1 and clean2.startswith(clean1):
                matches += 1.0
            elif len(clean2) == 1 and clean1.startswith(clean2):
                matches += 1.0
            elif fuzz.ratio(clean1, clean2) >= 85:
                matches += 0.9
        
        return matches / total if total > 0 else 0.0
    
    def _compare_dates(self, date1_str: str, date2_str: str) -> float:
        """Format-aware date comparison"""
        
        try:
            d1 = parser.parse(str(date1_str))
            d2 = parser.parse(str(date2_str))
            
            if d1 == d2:
                return 1.0
            if d1.year == d2.year and d1.month == d2.month:
                return 0.70
            if d1.year == d2.year:
                return 0.30
            
            return 0.0
        except Exception:
            return fuzz.ratio(str(date1_str), str(date2_str)) / 100.0
    
    def _compare_addresses(self, addr1: str, addr2: str) -> float:
        """Hierarchical address matching"""
        
        if not addr1 or not addr2:
            return 0.0
        
        full_score = fuzz.token_set_ratio(addr1.lower(), addr2.lower()) / 100.0
        
        if full_score > 0.8:
            return full_score
        
        parts1 = [p.strip() for p in addr1.lower().split(',')]
        parts2 = [p.strip() for p in addr2.lower().split(',')]
        
        if len(parts1) >= 2 and len(parts2) >= 2:
            city_score = fuzz.ratio(parts1[-1], parts2[-1]) / 100.0
            if city_score > 0.8:
                return 0.40
        
        return full_score * 0.5
    
    def _normalize_name(self, name: str) -> str:
        """Name normalization"""
        
        if not name:
            return ""
        
        name = name.lower().strip()
        
        titles = ["mr.", "mrs.", "ms.", "dr.", "prof.", "sir", "lord", "lady", "mr", "mrs", "ms", "dr"]
        for title in titles:
            if name.startswith(title + " ") or name.startswith(title + "."):
                name = name[len(title):].strip().lstrip('.')
        
        suffixes = ["jr.", "sr.", "ii", "iii", "iv", "esq.", "jr", "sr"]
        for suffix in suffixes:
            if name.endswith(" " + suffix) or name.endswith("," + suffix):
                name = name[:-(len(suffix))].strip().rstrip(',')
        
        if "," in name and name.count(",") == 1:
            parts = [p.strip() for p in name.split(",")]
            if len(parts) == 2:
                name = f"{parts[1]} {parts[0]}"
        
        name = " ".join(name.split())
        
        return name
    
    # ═══════════════════════════════════════════════════════
    # BLOCKING KEY (UNCHANGED - KEEP YOUR EXISTING CODE)
    # ═══════════════════════════════════════════════════════
    
    def create_blocking_key(self, entity: Dict) -> str:
        """Production-grade blocking key"""
        
        entity_type = (entity.get("entity_type") or "").lower()
        
        first_init, last = self._extract_name_parts(entity)
        dob = entity.get("dob") or entity.get("date_of_birth")
        city, ctry = self._extract_location(entity)
        
        prefix = "CO" if entity_type == "company" else "P"
        
        def is_valid(val):
            if val is None:
                return False
            val_str = str(val).lower()
            return val_str not in ['', 'nan', 'none', 'null']
        
        # PERSON BLOCKING
        if prefix == "P":
            if last and is_valid(dob):
                return f"{prefix}:{last[0]}:{dob}"
            
            if last and first_init and ctry:
                return f"{prefix}:{first_init}:{last}:{ctry}"
            
            if last and ctry:
                return f"{prefix}:{last}:{ctry}"
            
            if ctry:
                return f"{prefix}:{ctry}"
        
        # COMPANY BLOCKING
        else:
            company_num = entity.get("company_number", "")
            if is_valid(company_num):
                clean_num = str(company_num).strip()
                prefix_len = self._get_company_number_prefix_length(clean_num)
                if len(clean_num) >= prefix_len:
                    return f"{prefix}:NUM:{clean_num[:prefix_len]}"
            
            if city and ctry:
                city_norm = self._normalize_location(city)
                ctry_norm = self._normalize_location(ctry)
                return f"{prefix}:{city_norm}:{ctry_norm}"
            
            if ctry and last:
                industry = self._extract_industry_hint(entity.get("full_name", ""))
                if industry:
                    return f"{prefix}:{ctry}:{industry}"
                return f"{prefix}:{ctry}:{last[:3].upper()}"
            
            if ctry:
                return f"{prefix}:{ctry}"
        
        # FALLBACK
        name = self._normalize_name(entity.get("full_name", ""))
        if name:
            soundex = self._get_soundex_safe(name)
            if soundex:
                return f"{prefix}:SOUNDEX:{soundex}"
            
            parts = name.split()
            if len(parts) >= 2:
                return f"{prefix}:NAME:{parts[0][0]}{parts[-1][0]}".upper()
            else:
                return f"{prefix}:NAME:{parts[0][:2]}".upper() if parts else f"{prefix}:UNKNOWN"
        
        return f"{prefix}:UNKNOWN"
    
    def _extract_name_parts(self, entity: Dict) -> Tuple[Optional[str], Optional[str]]:
        """Extract name parts"""
        norm = self._normalize_name(entity.get("full_name", ""))
        if not norm:
            return None, None
        
        parts = norm.split()
        if len(parts) == 1:
            return parts[0][0].upper(), parts[0].upper()
        
        first = parts[0].replace('.', '').upper()
        last = parts[-1].upper()
        return (first[0] if first else None), last
    
    def _extract_location(self, entity: Dict) -> Tuple[Optional[str], Optional[str]]:
        """Extract location"""
        addr = self._safe_str(entity.get("address"))
        nat = self._safe_str(entity.get("nationality"))
        country = self._safe_str(entity.get("country"))
        
        city = addr.split()[0].upper() if addr else None
        
        if country:
            ctry = country.upper()
        elif nat:
            ctry = nat.upper()
        elif addr:
            ctry = addr.split()[-1].upper()
        else:
            ctry = None
        
        return city, ctry
    
    def _get_company_number_prefix_length(self, company_num: str) -> int:
        """Determine prefix length"""
        num_len = len(company_num)
        if num_len >= 8:
            return 3
        elif num_len >= 6:
            return 2
        else:
            return 1
    
    def _normalize_location(self, location: str) -> str:
        """Normalize location"""
        if not location:
            return ""
        
        loc = location.upper().strip()
        
        abbrev_map = {
            "ST.": "SAINT", "ST": "SAINT",
            "MT.": "MOUNT", "MT": "MOUNT",
            "FT.": "FORT", "FT": "FORT",
        }
        
        for abbrev, full in abbrev_map.items():
            if loc.startswith(abbrev):
                loc = loc.replace(abbrev, full, 1)
        
        loc = loc.replace(" ", "_")
        loc = loc.replace(".", "").replace(",", "").replace("-", "_")
        
        return loc
    
    def _extract_industry_hint(self, company_name: str) -> str:
        """Extract industry hint"""
        if not company_name:
            return ""
        
        name_lower = company_name.lower()
        
        industry_keywords = {
            "TECH": ["technology", "tech", "software", "digital"],
            "BANK": ["bank", "banking", "financial", "finance"],
            "TRADE": ["trading", "trade", "import", "export"],
            "PROP": ["property", "properties", "real estate", "holdings"],
        }
        
        for industry, keywords in industry_keywords.items():
            if any(kw in name_lower for kw in keywords):
                return industry
        
        return ""
    
    def _get_soundex_safe(self, name: str) -> str:
        """Get Soundex"""
        try:
            return jellyfish.soundex(name)
        except:
            return ""