#main.py
from helpers import init_update, create_directories,check_for_update

"""Esse arquivo vai conter a camada superior da aplicação, onde serão ligadas
as peças da máquina
"""

if __name__ == '__main__':
    if check_for_update():
        create_directories()
        init_update()
    