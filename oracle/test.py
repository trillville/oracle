import watchtower, logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('arp')
logger.addHandler(watchtower.CloudWatchLogHandler())


def main():
    # text_file = open("/home/ec2-user/oracle/oracle/test.txt", "w")
    # n = text_file.write("boowoop")
    # text_file.close()
    logger.info("boowoop")
    logger.info(dict(foo="arparp", details={}))

if __name__ == "__main__":
    main()
