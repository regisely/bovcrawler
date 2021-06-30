from easygui import *
import subprocess

code = enterbox("Code:")
quote = subprocess.check_output(['./getquote.sh', code])
msgbox(quote.split()[0].decode("utf-8") + ': ' + '{0:.2f}'.format(float(quote.split()[1])))
