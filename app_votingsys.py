import psycopg2, psycopg2.extras, os, json
from flask import Flask, jsonify, request, render_template, session, redirect, url_for, flash, send_from_directory, make_response
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import generate_password_hash, check_password_hash # 🚨 SECURITY FIX
from datetime import datetime
import pytz
from language_data import languages
import base64, io, numpy as np
from PIL import Image
#from deepface import DeepFace
from dotenv import load_dotenv

# --- Initialization ---
load_dotenv()

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_host=1, x_prefix=1) # 🚨 DEPLOYMENT FIX
app.secret_key = os.getenv("FLASK_VOTER_SECRET_KEY", "a_secret_key_for_voter_sessions")
app.config['SESSION_COOKIE_SECURE'] = True # 🚨 SECURE COOKIE FIX
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
IST = pytz.timezone('Asia/Kolkata')
UTC = pytz.utc

# --- CODE COMPARISON FUNCTION: REMOVED (Replaced by check_password_hash) ---

# --- DB helper ---
def get_db():
    """Establishes a connection to the PostgreSQL database using .env credentials."""
    try:
        conn = psycopg2.connect(
            # 🚨 FIX: Using DB_NAME for consistency and passing SSL mode
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            sslmode='require' # 🚨 CRITICAL SSL FIX
        )
        return conn
    except psycopg2.OperationalError as e:
        app.logger.error(f"Error connecting to PostgreSQL database: {e}")
        return None
    except Exception as e:
        app.logger.error(f"General DB connection error: {e}")
        return None
    
# --- Utility: numeric sort ---
def numeric_sort(arr):
    def parse_num(s):
        s = ''.join(filter(str.isdigit, str(s)))
        return int(s) if s else 0
    return sorted(arr, key=parse_num)

# --- Helper Function to Determine Household Query Clause ---
def get_household_where_clause(data):
    """
    Builds the WHERE clause and params list needed to uniquely identify 
    a household based on incoming API data (address components).
    Returns: (where_clauses_list, params_list)
    """
    society = data.get('society')
    if not society:
        return ([], [])

    where_clauses = ["society_name=%s"]
    params = [society]
    
    if data.get('tower') and data.get('flat'):
        where_clauses.extend(["tower=%s", "flat=%s"])
        params.extend([data.get('tower'), data.get('flat')])
    elif data.get('lane') and data.get('house'):
        where_clauses.extend(["lane=%s", "house_number=%s"])
        params.extend([data.get('lane'), data.get('house')])
    elif data.get('flat') and not (data.get('tower') or data.get('lane')): # Individual no lanes
        where_clauses.append("flat=%s")
        params.append(data.get('flat'))
    elif not (data.get('tower') or data.get('flat') or data.get('lane') or data.get('house')):
        where_clauses.extend(["tower IS NULL", "flat IS NULL", "lane IS NULL", "house_number IS NULL"])
    else:
        return ([], []) # Incomplete or unmatchable details

    return (where_clauses, params)

# --- Select language ---
@app.route("/", methods=["GET","POST"])
def select_language():
    session.clear()
    if request.method=="POST":
        lang_code = request.form.get('lang')
        if lang_code in languages:
            session['lang']=lang_code
            return redirect(url_for('login'))
    resp = make_response(render_template("select_language.html", languages=languages))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp

# --- Login page ---
@app.route("/login", methods=["GET","POST"])
def login():
    # 🛑 THE FIX: Force the user to select a language if session['lang'] is missing.
    lang = session.get('lang', None)
    if not lang:
        flash("Please select a language first.", "warning")
        return redirect(url_for('select_language'))
        
    societies = []
    conn = get_db()
    if conn:
        try:
            with conn.cursor() as cur:
                # Assuming 'settings' table has the society_name
                cur.execute("SELECT DISTINCT society_name FROM settings ORDER BY society_name") 
                societies = [row[0] for row in cur.fetchall()]
        except Exception as e:
            app.logger.error(f"Error fetching societies in /login: {e}")
        finally:
            conn.close()

    if request.method=="POST":
        flash("Please use verification options.", "info")
        return redirect(url_for('login'))

    # If we reach here, lang is the correctly selected language code.
    resp = make_response(render_template(
        "vote.html", 
        # 🚨 FINAL FIX HERE: Pass the fetched list of societies
        societies=societies, 
        community_data={}, 
        languages=languages, 
        selected_language_code=lang
    ))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp

# --- API: Get society details ---
@app.route("/api/get_society_details", methods=["POST"])
def get_society_details():
    data = request.get_json()
    society_name = data.get('society')
    if not society_name:
        return jsonify({"success": False, "message": "Society name required."}), 400

    conn = get_db()
    if not conn:
        return jsonify({"success": False, "message": "DB connection error."}), 500

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Fetch housing type
            cur.execute("SELECT housing_type FROM settings WHERE society_name=%s", (society_name,))
            setting = cur.fetchone()
            if not setting or not setting['housing_type']:
                return jsonify({"success": False, "message": "Society not found."}), 404

            housing_type = setting['housing_type'].lower()

            # Apartments
            if 'apartment' in housing_type:
                cur.execute("""
                    SELECT tower, flat FROM households
                    WHERE society_name=%s
                    ORDER BY tower, flat
                """, (society_name,))
                rows = cur.fetchall()
                if not rows:
                    return jsonify({"success": False, "message": "No households found."}), 404

                community_structure = {}
                for r in rows:
                    tower, flat = r['tower'], str(r['flat'])
                    floor = 'GF'
                    digits = ''.join(filter(str.isdigit, flat))
                    if len(digits) > 2:
                        floor = digits[:-2]
                    community_structure.setdefault(tower, {}).setdefault(floor, []).append(flat)

                # Sort floors/flats
                for t in community_structure:
                    community_structure[t] = {k: numeric_sort(v) for k, v in community_structure[t].items()}

                return jsonify({
                    "success": True,
                    "community_type": "apartment",
                    "community_data": community_structure
                })

            # Individual with lanes — use tower as lane name, flat as house number
            elif 'lanes' in housing_type:
                cur.execute("""
                    SELECT tower AS lane, flat AS house_number FROM households
                    WHERE society_name=%s
                    ORDER BY lane, house_number
                """, (society_name,))
                rows = cur.fetchall()
                if not rows:
                    return jsonify({"success": False, "message": "No households found."}), 404

                lane_structure = {}
                for r in rows:
                    lane, house = r['lane'], str(r['house_number'])
                    lane_structure.setdefault(lane, []).append(house)

                lane_structure = {k: numeric_sort(v) for k, v in lane_structure.items()}

                return jsonify({
                    "success": True,
                    "community_type": "individual_lanes",
                    "community_data": lane_structure
                })

            # Individual no lanes — single dropdown
            else:
                cur.execute("""
                    SELECT DISTINCT COALESCE(flat::text, '') AS flat
                    FROM households
                    WHERE society_name=%s
                    ORDER BY flat
                """, (society_name,))
                rows = cur.fetchall()
                # Sort numerically, then convert back to string
                flats = numeric_sort([r['flat'] for r in rows if r['flat']])
                return jsonify({
                    "success": True,
                    "community_type": "individual_no_lanes",
                    "community_data": {"flats": flats} # Put flats in a dictionary
                })

    except Exception as e:
        app.logger.error(f"Error get_society_details: {e}", exc_info=True)
        return jsonify({"success": False, "message": "Server error fetching society details."}), 500

    finally:
        if conn:
            conn.close()

# --- Verification: Secret Code (MODIFIED for Hashing) ---
@app.route("/api/verify_code", methods=["POST"])
def verify_code():
    data = request.get_json()
    if not isinstance(data, dict):
        return jsonify({"success": False, "message": "Invalid request format."}), 400
        
    society = data.get('society')
    entered_code = data.get('secret_code')
    mode = data.get('mode', 'vote') 

    if not society or not entered_code:
        return jsonify({"success": False, "message": "Society and secret code required"}), 400
    
    where_clauses, params = get_household_where_clause(data)
    if not where_clauses:
        return jsonify({"success": False, "message": "Incomplete household details"}), 400
    
    conn = get_db()
    if not conn:
        return jsonify({"success": False, "message": "DB connection error"}), 500

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            
            # 1. Fetch Voting Schedule (needed for vote mode)
            cur.execute("SELECT start_time, end_time FROM voting_schedule WHERE society_name=%s", (society,))
            sched = cur.fetchone()
            if not sched or not sched['start_time'] or not sched['end_time']:
                return jsonify({"success": False, "message": "Voting schedule not set"}), 403

            # 2. Fetch Household Record using address components
            query = "SELECT id, secret_code, reset_code, is_admin_blocked, is_vote_allowed, voted_in_cycle, voted_at FROM households WHERE " + " AND ".join(where_clauses)
            cur.execute(query, tuple(params))
            h = cur.fetchone()
            
            if not h:
                return jsonify({"success": False, "message": "Household not found with provided details."}), 410

            voter_secret_code = h.get('secret_code')
            voter_reset_code = h.get('reset_code')
            
            # --- TIERED CODE VALIDATION LOGIC (NOW USING HASHING) ---
            is_valid_code = False
            is_reset_required = False
            should_nullify_secret_code = False 
            
            # Check 1: Mandatory Reset Case (secret_code set, reset_code is NULL)
            if voter_secret_code and voter_reset_code is None:
                # 🚨 SECURITY FIX: Check entered code against the stored HASH
                if check_password_hash(voter_secret_code, entered_code):
                    is_reset_required = True
                    is_valid_code = True 
            
            # Check 2: Verification using User-Set Reset Code (reset_code is primary/permanent)
            elif voter_reset_code:
                # 🚨 SECURITY FIX: Check entered code against the stored HASH
                if check_password_hash(voter_reset_code, entered_code):
                    is_valid_code = True
            
            # Check 3: Fallback to Admin/Mobile Secret Code (Used for one-time entry)
            elif voter_secret_code:
                # 🚨 SECURITY FIX: Check entered code against the stored HASH
                if check_password_hash(voter_secret_code, entered_code):
                    is_valid_code = True
                    # Mark the temporary secret_code for immediate nullification after successful use
                    should_nullify_secret_code = True
            
            
            if not is_valid_code:
                return jsonify({"success": False, "message": "Invalid code."}), 410

            if is_reset_required:
                # Code verified, but reset is mandatory.
                return jsonify({"success": True, "needs_reset": True, "message": "Code reset required."})

            # --- END TIERED CODE VALIDATION LOGIC ---
            
            # 3. Standard Pre-checks (Apply to both modes)
            if h['is_admin_blocked']:
                return jsonify({"success": False, "message": "Blocked by admin"}), 403
            if not h['is_vote_allowed']:
                return jsonify({"success": False, "message": "Voting not allowed"}), 403

            # 4. Mode Specific Checks (Voting vs. Checking)
            proof_timestamp_str = None
            if h['voted_in_cycle'] == 1 and h.get('voted_at'):
                # Handle time zone conversion for display
                voted_at = h['voted_at']
                if voted_at.tzinfo is None:
                    voted_at = pytz.utc.localize(voted_at)
                voted_time_ist = voted_at.astimezone(IST)
                proof_timestamp_str = voted_time_ist.strftime('%d-%m-%Y %I:%M:%S %p %Z')

            if mode == 'vote':
                # Check voting window
                try:
                    start_time_raw = sched['start_time']
                    end_time_raw = sched['end_time']
                    
                    if isinstance(start_time_raw, str): start_time_raw = start_time_raw.replace('Z', '+00:00')
                    if isinstance(end_time_raw, str): end_time_raw = end_time_raw.replace('Z', '+00:00')
                    
                    start_time = start_time_raw if isinstance(start_time_raw, datetime) else datetime.fromisoformat(start_time_raw)
                    end_time = end_time_raw if isinstance(end_time_raw, datetime) else datetime.fromisoformat(end_time_raw)

                    if start_time.tzinfo is None: start_time = start_time.replace(tzinfo=pytz.utc)
                    if end_time.tzinfo is None: end_time = end_time.replace(tzinfo=pytz.utc)

                    if not (start_time <= datetime.now(pytz.utc) < end_time):
                        return jsonify({"success": False, "message": "Voting is closed"}), 403
                except Exception as e:
                    app.logger.error(f"Date parsing error: {e}")
                    return jsonify({"success": False, "message": "Invalid date format or timezone error"}), 400

                # CRITICAL FIX: Explicitly handle the 'already voted' case before the final success block.
                if h['voted_in_cycle'] == 1:
                    # Return success with the timestamp. The JavaScript handles the display.
                    return jsonify({
                        "success": True, 
                        "voted_at": proof_timestamp_str,
                        "redirect_url": url_for('ballot') 
                    })

            # 5. Success Response
            
            # --- IMPLEMENT SINGLE-USE NULLIFICATION HERE ---
            if should_nullify_secret_code:
                # Invalidate the one-time mobile code now that the user has successfully entered
                cur.execute("UPDATE households SET secret_code = NULL WHERE id = %s", (h['id'],))
                conn.commit() # Commit the token invalidation immediately
            # --- END SINGLE-USE NULLIFICATION ---
            
            session['household_id'] = h['id']
            session['society_name'] = society
            
            return jsonify({
                "success": True,
                "message": "Verification successful",
                "voted_at": proof_timestamp_str,
                "redirect_url": url_for('ballot')
            })

    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Verify code error: {e}", exc_info=True)
        return jsonify({"success": False, "message": "Server error: " + str(e)}), 500
    finally:
        if conn:
            conn.close()
            
# --- New API: Reset Code (MODIFIED for Hashing) ---
@app.route('/api/reset_code', methods=['POST'])
def reset_code():
    data = request.get_json()
    if not isinstance(data, dict):
        return jsonify({"success": False, "message": "Invalid request format."}), 400
        
    user_id = data.get('user_id')
    new_code = data.get('new_code')
    confirm_code = data.get('confirm_code')

    if not user_id or not new_code or not confirm_code:
        return jsonify({"success": False, "message": "Missing required fields."}), 400
        
    if new_code != confirm_code:
        return jsonify({"success": False, "message": "New codes do not match."})

    conn = get_db()
    if not conn:
        return jsonify({"success": False, "message": "DB connection error"}), 500

    try:
        # Re-parse user_id components from the string
        parts = user_id.split('-')
        if len(parts) < 2:
            return jsonify({"success": False, "message": "Invalid user identifier format."}), 400
            
        society = parts[0]
        
        # Build WHERE clause from composite user_id string
        where_clauses = ["society_name=%s"]
        params = [society]

        if len(parts) == 4: # Apartment (society-tower-floor-flat) -> Use tower and flat
            where_clauses.extend(["tower=%s", "flat=%s"])
            params.extend([parts[1], parts[3]]) 
        elif len(parts) == 3: # Individual lanes (society-lane-house)
            where_clauses.extend(["lane=%s", "house_number=%s"])
            params.extend([parts[1], parts[2]]) 
        elif len(parts) == 2: # Individual no lanes (society-flat)
            where_clauses.append("flat=%s")
            params.extend([parts[1]])
        else:
            return jsonify({"success": False, "message": "Invalid user identifier format."}), 400
        
        # 🚨 SECURITY FIX: Hash the new code before storing it
        hashed_code = generate_password_hash(new_code)
        
        # Update the database: Set the new hashed code into 'reset_code'
        update_query = "UPDATE households SET reset_code=%s WHERE " + " AND ".join(where_clauses)
        update_params = [hashed_code] + params # Use the HASHED code
        
        cur = conn.cursor()
        cur.execute(update_query, tuple(update_params))
        
        if cur.rowcount == 1:
            conn.commit()
            return jsonify({
                "success": True, 
                "message": "Code reset successful! Please log in with your new code."
            })
        else:
            return jsonify({"success": False, "message": "Failed to update household (record not found or update failed)."}), 404

    except Exception as e:
        app.logger.error(f"Reset code error: {e}", exc_info=True)
        return jsonify({"success": False, "message": "Server error during reset."}), 500
    finally:
        if conn:
            conn.close()

# --- Verification: Face ---
@app.route("/api/verify_face", methods=["POST"])
def verify_face():
    # Face verification logic is commented out/disabled.
    return jsonify({"verified": False, "message": "Face verification temporarily disabled"}), 200

# --- Ballot page ---
@app.route("/ballot")
def ballot():
    if "household_id" not in session:
        flash("Session expired", "error"); return redirect(url_for("login"))
    household_id=session['household_id']
    conn=get_db()
    if not conn: flash("DB error","danger"); return redirect(url_for("login"))
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT voted_in_cycle,society_name,tower FROM households WHERE id=%s",(household_id,))
            h=cur.fetchone()
            if h['voted_in_cycle']==1: session.clear(); flash("Already voted","info"); return redirect(url_for("select_language"))
            society=h['society_name']; tower=h['tower']

            cur.execute("SELECT max_candidates_selection,is_towerwise FROM settings WHERE society_name=%s",(society,))
            s=cur.fetchone(); max_sel=s['max_candidates_selection'] if s else 1; is_towerwise=s['is_towerwise'] if s else False

            if is_towerwise and tower:
                cur.execute("SELECT contestant_name,contestant_symbol,contestant_photo_b64 FROM households WHERE is_contestant=1 AND society_name=%s AND tower=%s ORDER BY contestant_name",(society,tower))
            else:
                cur.execute("SELECT contestant_name,contestant_symbol,contestant_photo_b64 FROM households WHERE is_contestant=1 AND society_name=%s ORDER BY contestant_name",(society,))
            contestants=cur.fetchall()
            if not contestants: flash("No contestants","error"); return redirect(url_for("login"))
            contestants_data=[{"name":c["contestant_name"],"symbol":c["contestant_symbol"],"photo_b64":c["contestant_photo_b64"]} for c in contestants]
    finally:
        conn.close()
    lang=session.get('lang','en')
    resp=make_response(render_template("ballot.html",contestants=contestants_data,maxSelections=max_sel,languages=languages,society_name=society,selected_language_code=lang,tower_name=tower))
    resp.headers['Cache-Control']='no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma']='no-cache'
    resp.headers['Expires']='0'
    return resp

# --- Submit vote ---
@app.route("/submit_vote",methods=["POST"])
def submit_vote():
    if "household_id" not in session: return jsonify({"success":False,"message":"Session expired"}),401
    household_id=session['household_id']; society=session.get('society_name')
    if not society: return jsonify({"success":False,"message":"Missing society info"}),400
    data=request.get_json(); selected=data.get("contestants")
    if not selected: return jsonify({"success":False,"message":"No contestants selected"}),400

    conn=get_db()
    if not conn: return jsonify({"success":False,"message":"DB error"}),500
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT tower,voted_in_cycle FROM households WHERE id=%s",(household_id,))
            h=cur.fetchone(); VOTED_FLAG=1
            if h['voted_in_cycle']==VOTED_FLAG: return jsonify({"success":False,"message":"Already voted"}),403
            tower=h['tower'] if h else None
            cur.execute("SELECT max_voters,voted_count FROM settings WHERE society_name=%s",(society,))
            s=cur.fetchone()
            if not s: return jsonify({"success":False,"message":"Settings not found"}),500
            if s['voted_count']>=s['max_voters']: return jsonify({"success":False,"message":"Max votes reached"}),403

            for c in selected:
                cur.execute("""INSERT INTO votes (society_name,tower,contestant_name,is_archived,vote_count)
                                 VALUES (%s,%s,%s,%s,1)
                                 ON CONFLICT (society_name,tower,contestant_name,is_archived)
                                 DO UPDATE SET vote_count=votes.vote_count+1""",(society,tower,c,0))
            cur.execute("UPDATE settings SET voted_count=voted_count+1 WHERE society_name=%s",(society,))
            voted_timestamp = datetime.now(pytz.utc)
            cur.execute("UPDATE households SET voted_in_cycle=%s, voted_at=%s WHERE id=%s",(VOTED_FLAG, voted_timestamp, household_id))
        conn.commit(); session.pop('household_id',None); session.pop('society_name',None)
        msg=languages.get(session.get('lang','en'),{}).get('voteSuccess','Vote successfully cast!')
        return jsonify({"success":True,"message":msg})
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Submit vote error: {e}",exc_info=True)
        return jsonify({"success":False,"message":"Server error"}),500
    finally:
        if conn: conn.close()

# --- Main ---
if __name__=='__main__':
    app.run(port=5001,debug=False) 