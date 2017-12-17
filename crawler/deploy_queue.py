from deploy import *

def main():
    master_node = run_master_node()
    deploy_code(master_node)
    configure_python(master_node)
    setup_graphite(master_node)
    start_urlqueue(master_node)

if __name__ == main():
    main()
