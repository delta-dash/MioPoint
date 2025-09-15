import os
os.environ["KCFG_KIVY_LOG_LEVEL"] = "warning"

import asyncio
import sys
from pathlib import Path

# Kivy-related imports are now inside start_kivy to prevent
# initialization when the module is just imported.
# This part is technically optional but is good practice.

# Your project-specific imports
from src.models import UserCreate
import logging
from src.roles.rbac_manager import assign_role_to_user, get_role_id
from src.users.user_manager import create_user

logger = logging.getLogger("AdminSetupUI")
logger.setLevel(logging.DEBUG)

KV = """
MDScreen:
    md_bg_color: app.theme_cls.bg_normal

    MDCard:
        orientation: "vertical"
        padding: "30dp"
        spacing: "25dp"
        size_hint: .85, None
        height: self.minimum_height
        pos_hint: {"center_x": .5, "center_y": .5}
        elevation: 12
        radius: [20, 20, 20, 20]

        BoxLayout:
            orientation: "vertical"
            spacing: "10dp"
            size_hint_y: None
            height: self.minimum_height

            MDIcon:
                icon: "shield-account"
                halign: "center"
                font_size: "72sp"
                theme_text_color: "Custom"
                text_color: app.theme_cls.primary_color

            MDLabel:
                text: "Admin Setup"
                halign: "center"
                font_style: "H5"
                bold: True

            Widget:
                size_hint_y: None
                height: "8dp"

            MDLabel:
                text: "Create the first user account with administrator privileges."
                halign: "center"
                theme_text_color: "Secondary"
                font_style: "Body2"

        MDTextField:
            id: username
            hint_text: "Username"
            icon_left: "account-circle"
            mode: "fill"
            helper_text: "Choose a unique username"
            helper_text_mode: "on_focus"

        MDTextField:
            id: password
            hint_text: "Password"
            password: True
            icon_left: "key-variant"
            mode: "fill"
            helper_text: "Use at least 8 characters"
            helper_text_mode: "on_focus"

        MDFillRoundFlatButton:
            text: "Create Admin User"
            pos_hint: {"center_x": 0.5}
            font_size: "16sp"
            on_release: app.register_user()
"""

async def create_admin_in_db(username, password):
    """Handles the async database operations to create an admin."""
    try:
        user_data = UserCreate(username=username, password=password)
        user_id = await create_user(user_data)

        if user_id is None:
            return False, "User creation failed. The username might already exist."

        admin_role_id = await get_role_id('Admin')
        if admin_role_id is None:
            return False, "Critical Error: 'Admin' role not found in the database."

        success = await assign_role_to_user(user_id, admin_role_id)

        if success:
            logger.info(f"Successfully created admin user '{username}'.")
            return True, f"Admin user '{username}' was created successfully."
        else:
            logger.error("User was created, but failed to assign the Admin role.")
            return False, "User was created, but failed to assign the Admin role."

    except Exception as e:
        logger.error(f"An exception occurred during admin creation: {e}", exc_info=True)
        return False, f"An unexpected error occurred: {e}"

def start_kivy(completion_event=None):
    """
    Creates and returns an instance of the Kivy application.
    It now accepts an asyncio.Event to signal completion.
    """
    from kivymd.app import MDApp
    from kivy.lang import Builder
    from kivymd.uix.dialog import MDDialog
    from kivymd.uix.button import MDRaisedButton
    from kivy.core.window import Window
    class RegisterApp(MDApp):
        def __init__(self, completion_event, **kwargs):
            super().__init__(**kwargs)
            # Store the event from main.py
            self.completion_event = completion_event
            # We no longer need registration_task or exit_flag
            self.dialog = None

        def build(self):
            self.theme_cls.theme_style = "Light"
            self.theme_cls.primary_palette = "Indigo"
            return Builder.load_string(KV)

        def register_user(self):
            """
            Synchronous UI callback that launches the asynchronous registration logic.
            """
            username = self.root.ids.username.text.strip()
            password = self.root.ids.password.text.strip()

            if not username or not password:
                self.show_dialog("Missing Info", "Please provide both a username and password.")
                return
            if len(password) < 8:
                self.show_dialog("Weak Password", "Password must be at least 8 characters long.")
                return

            # Launch the async task from the UI thread. This is the correct pattern.
            asyncio.create_task(self.run_registration_logic(username, password))

        async def run_registration_logic(self, username, password):
            """
            The actual async part that awaits the database operation
            and then shows the result.
            """
            logger.info("Running registration logic asynchronously.")
            success, message = await create_admin_in_db(username, password)
            self.process_registration_result(success, message)

        def process_registration_result(self, success, message):
            """
            Displays the final result to the user.
            This is now called from run_registration_logic.
            Since the whole app is running under Kivy's asyncio loop, this is safe.
            """
            if success:
                self.show_dialog("Registration Complete", message, exit_on_close=True)
            else:
                self.show_dialog("Error", message)

        def show_dialog(self, title, text, exit_on_close=False):
            if self.dialog:
                self.dialog.dismiss()

            def on_ok_button_press(instance):
                self.dialog.dismiss()
                Window.close()
                if exit_on_close:
                    # Signal to main.py that we are done.
                    if self.completion_event:
                        self.completion_event.set()
                    # The stop() will cause async_run() in main.py to finish.
                    self.stop()

            self.dialog = MDDialog(
                title=title,
                text=text,
                buttons=[
                    MDRaisedButton(
                        text="OK",
                        on_release=on_ok_button_press
                    )
                ],
            )
            self.dialog.open()

        def on_stop(self):
            """Ensure the event is set even if the user closes the window."""
            if self.completion_event and not self.completion_event.is_set():
                self.completion_event.set()

    # Pass the event into the app's constructor.
    return RegisterApp(completion_event=completion_event)
