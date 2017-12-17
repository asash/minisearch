from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse

def init_robots(allowed_domains):
    robot_filters = {}
    for domain in allowed_domains:
        rp = RobotFileParser()
        url = "https://{}/robots.txt".format(domain)
        rp.set_url(url)
        rp.read()
        robot_filters[domain] = rp

    def crawl_prohibited(robots, url):
        domain = urlparse(url).netloc
        if domain not in robots:
            return True
        return not robots[domain].can_fetch('*', url)

    return lambda url : crawl_prohibited(robot_filters, url)



    
