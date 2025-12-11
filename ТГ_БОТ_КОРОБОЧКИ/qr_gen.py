import qrcode

url = "https://t.me/Korobochka2_bot?start"
img = qrcode.make(url)
img.save("korobochka_qr.png")
