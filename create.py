from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

text = """
City Care Hospital
Dr. R. Sharma, MD
Reg No: 456789

Patient Name: Lakshya Yadav
Age: 24
Date: 27/01/2026

Rx:

1. Paracetamol 500mg
   Take one tablet twice daily after food for 5 days

2. Ibuprofen 400mg
   Take one tablet once daily after food for 3 days

3. Amoxicillin 250mg
   Take one capsule three times daily for 7 days

Notes:
Drink plenty of water.
Rest advised.

Doctor Signature
---------------------

"""

c = canvas.Canvas("sample_prescription.pdf", pagesize=A4)
c.drawString(50, 800, text)
c.save()
