from flask import Flask, render_template, request, redirect, url_for, session, jsonify, flash
import os
import datetime
from dotenv import load_dotenv

# Load environment variables BEFORE importing config
load_dotenv()

from neo_vault import NeoVault
from neo_email import NeoEmailSystem
import pyotp
from config import EMAIL_CONFIG

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'd5bcfffe053e1f7be1911665161e086980e2dab12fe8015e376b0def622f785b')

# Initialize systems
vault = NeoVault()
email_system = NeoEmailSystem(vault, EMAIL_CONFIG)
email_system.start()

@app.route('/')
def home():
    if 'user_id' in session:
        return redirect(url_for('dashboard'))
    return render_template('index.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        totp_code = request.form.get('totp_code')

        if not username or not password:
            flash('Username and password required')
            return render_template('login.html')

        result = vault.authenticate_user(username, password)

        if not result['success']:
            flash(result['error'])
            return render_template('login.html')

        if result.get('requires_2fa'):
            if not totp_code:
                flash('2FA code required')
                return render_template('login.html', requires_2fa=True, user_id=result['user_id'])

            # Verify 2FA
            secret = vault.get_2fa_secret(result['user_id'])
            if not secret or not pyotp.TOTP(secret).verify(totp_code):
                flash('Invalid 2FA code')
                return render_template('login.html', requires_2fa=True, user_id=result['user_id'])

        # Login successful
        session['user_id'] = result['user_id']
        session['username'] = username

        # Send login notification email
        user_email = vault.conn.execute("SELECT email FROM users WHERE id = ?", (result['user_id'],)).fetchone()
        if user_email:
            email_system.send_template_email(
                to_email=user_email[0],
                template_name="login_notification",
                context={"username": username, "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
                priority=1,
                user_id=result['user_id']
            )

        flash('Login successful')
        return redirect(url_for('dashboard'))

    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        email = request.form.get('email')

        if not username or not password or not email:
            flash('All fields required')
            return render_template('register.html')

        # Register user
        verification_code = vault.create_user(username, password, email)
        if verification_code:
            # Get user ID for email
            user = vault.conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
            if user:
                user_id = user[0]
                # Queue verification email
                email_system.send_template_email(
                    to_email=email,
                    template_name="verification",
                    context={"username": username, "code": verification_code},
                    priority=2,  # High priority for verification
                    user_id=user_id
                )
                # Trigger welcome workflow
                email_system.trigger_workflow("user_registered", user_id, {"username": username})
            flash('Registration successful. Please check your email for verification code.')
            return redirect(url_for('login'))
        else:
            flash('Username or email already exists')
            return render_template('register.html')

    return render_template('register.html')

@app.route('/dashboard')
def dashboard():
    if 'user_id' not in session:
        return redirect(url_for('login'))

    user_id = session['user_id']
    # Get user data
    skills = vault.get_user_skills()
    sessions = vault.get_recent_sessions(user_id, limit=10)
    return render_template('dashboard.html', skills=skills, sessions=sessions)

@app.route('/verify/<code>')
def verify(code):
    # Find user with this verification code
    result = vault.conn.execute("SELECT username FROM users WHERE verification_code = ?", (code,)).fetchone()
    if result:
        username = result[0]
        vault.verify_user(username, code)
        # Trigger post-verification workflow
        user = vault.conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
        if user:
            email_system.trigger_workflow("user_verified", user[0], {"username": username})
        flash('Account verified successfully. You can now login.')
        return redirect(url_for('login'))
    else:
        flash('Invalid verification code.')
        return redirect(url_for('home'))

@app.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    if request.method == 'POST':
        email = request.form.get('email')
        if not email:
            flash('Email required')
            return render_template('forgot_password.html')

        # Generate recovery code
        recovery_code = vault.generate_recovery_code(email)
        if recovery_code:
            # Queue recovery email
            email_system.send_template_email(
                to_email=email,
                template_name="password_recovery",
                context={"code": recovery_code},
                priority=2
            )
            flash('Password recovery email sent. Check your email.')
            return redirect(url_for('login'))
        else:
            flash('Email not found')
            return render_template('forgot_password.html')

    return render_template('forgot_password.html')

@app.route('/reset-password/<code>', methods=['GET', 'POST'])
def reset_password(code):
    if request.method == 'POST':
        new_password = request.form.get('new_password')
        confirm_password = request.form.get('confirm_password')

        if not new_password or not confirm_password:
            flash('All fields required')
            return render_template('reset_password.html', code=code)

        if new_password != confirm_password:
            flash('Passwords do not match')
            return render_template('reset_password.html', code=code)

        # Get email from recovery code
        result = vault.conn.execute(
            "SELECT email FROM users WHERE recovery_code = ? AND recovery_expiry > ?",
            (code, datetime.datetime.now().isoformat())
        ).fetchone()

        if result:
            email = result[0]
            if vault.reset_password(email, new_password):
                flash('Password reset successfully. You can now login.')
                return redirect(url_for('login'))
            else:
                flash('Failed to reset password')
        else:
            flash('Invalid or expired recovery code')

        return render_template('reset_password.html', code=code)

    return render_template('reset_password.html', code=code)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('home'))

if __name__ == '__main__':
    app.run(debug=True)
