import asyncio
import os
import json
import time
from datetime import datetime
from pymongo import MongoClient
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.errors import FloodWait, UserIsBlocked, ChatWriteForbidden

# Load from environment variables
API_ID = int(os.getenv('TELEGRAM_API_ID', '28620311'))
API_HASH = os.getenv('TELEGRAM_API_HASH', '3b5c4ed0598e48fc1ab552675555e693')
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '7425168690:AAEUJnjKry6XUdukb1OiGUGXfWNE-V6HHSU')
MONGO_URL = os.getenv('MONGO_URL', 'mongodb://Aditya:0099@cluster0-shard-00-00.vvcyc.mongodb.net:27017,cluster0-shard-00-01.vvcyc.mongodb.net:27017,cluster0-shard-00-02.vvcyc.mongodb.net:27017/?ssl=true&authSource=admin&retryWrites=true&w=majority')

# Admin user IDs (ye change kar lo apne admin IDs se)
ADMIN_IDS = [7355827552]  # Apna user ID yahan add karo

ACTIVE_CHATS_FILE = 'chats.json'
FAILED_CHATS_FILE = 'failed_chats.json'

app = Client("broadcast_bot", API_ID, API_HASH, bot_token=BOT_TOKEN)

class BroadcastSystem:
    def __init__(self):
        self.mongo_client = MongoClient(MONGO_URL)
        self.anon_db = self.mongo_client["Anon"]
        
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
            "🎯 **Broadcast Commands:**\n"
            "• Koi bhi message, photo, video, document forward karo broadcast ke liye\n"
            "• Message me text, caption, buttons, formatting sab copy hoga\n\n"
            "📊 **Other Commands:**\n"
            "• /stats - Database statistics\n"
            "• /broadcast_stats - Broadcast statistics\n\n"
            "⚡ **Features:**\n"
            "✅ Support all message types\n"
            "✅ Automatic rate limiting\n"
            "✅ Failed chats tracking\n"
            "✅ Real-time progress updates"
        )
    else:
        text = "👋 Hello! I'm a broadcast bot. Only admins can use me."
    
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

# Broadcast handler - Koi bhi message forward karo
@app.on_message(filters.private & ~filters.command(["start", "stats", "broadcast_stats", "clear_failed"]))
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
        "Type: `yes` to confirm"
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
@app.on_message(filters.group)
async def group_broadcast_handler(client, message: Message):
    user_id = message.from_user.id
    
    # Check if admin
    if user_id not in ADMIN_IDS:
        return
    
    # Check if already broadcasting
    if broadcast_system.is_broadcasting:
        await message.reply("⚠️ Ek broadcast already chal raha hai!")
        return
    
    # Check if message has /broadcast command or is a reply to bot
    if message.text and message.text.startswith("/broadcast"):
        # Get message to broadcast (replied message or next message)
        if message.reply_to_message:
            broadcast_msg = message.reply_to_message
        else:
            await message.reply("⚠️ Kisi message ko reply karke /broadcast use karo")
            return
        
        status_msg = await message.reply("⏳ Broadcast start ho raha hai...")
        
        # Start broadcast
        await broadcast_system.start_broadcast(broadcast_msg, status_msg)

print("✅ Bot is starting...")
print(f"👤 Admin IDs: {ADMIN_IDS}")
print("📡 Bot is now running...")

# Run bot
app.run()
