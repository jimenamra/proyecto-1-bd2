from backend.core.dispatcher import CommandDispatcher
from backend.core.command_parser import CommandParser

def main():
    parser = CommandParser()
    dispatcher = CommandDispatcher()

    while True:
        texto = input("🧠 Escribe tu instrucción: ")
        if texto.strip().lower() == "salir":
            break
        if texto.strip().lower().startswith("select"):
            resultados = dispatcher.execute_sql(texto)
            print(f"📊 {len(resultados)} eventos encontrados por SQL:")
            for ev in resultados:
                print(f"- {ev.descripcion}")
        try:
            comando = parser.parse(texto)
            resultado = dispatcher.execute(comando)
            print(resultado)
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
