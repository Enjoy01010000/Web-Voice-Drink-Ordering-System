# Smart Voice Drink Ordering System

ระบบสั่งเครื่องดื่มด้วยคำสั่งเสียงและควบคุมการจ่ายเครื่องดื่มผ่าน MQTT

## ภาพรวม
- เว็บแอป Flask สำหรับเลือกเมนูเครื่องดื่มและระดับความหวาน
- รองรับการสั่งงานด้วยเสียงผ่าน Web Speech API
- สื่อสารกับเครื่องจ่ายผ่าน MQTT
- สร้าง QR PromptPay และตรวจสอบสลิปผ่าน SlipOK
- บันทึกคำสั่งซื้อด้วย SQLite

## เริ่มต้นใช้งาน
1. ติดตั้ง Python 3.9 ขึ้นไป
2. ติดตั้ง dependencies ด้วยคำสั่ง pip install -r requirements.txt
3. คัดลอก config.example.json เป็น config.json แล้วกรอกค่าการเชื่อมต่อของคุณ
4. รัน python app.py
5. เปิด http://localhost:5000 (ตรวจสอบพอร์ตจากการตั้งค่าในแอป)

## ความปลอดภัย
ห้าม commit config.json, .env, ฐานข้อมูล และ log ที่มีข้อมูลจริง ใช้ config.example.json เป็นแม่แบบเท่านั้น
