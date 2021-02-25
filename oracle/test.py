def main():
    text_file = open("/home/ec2-user/oracle/oracle/test.txt", "w")
    n = text_file.write("boowoop")
    text_file.close()

if __name__ == "__main__":
    main()
