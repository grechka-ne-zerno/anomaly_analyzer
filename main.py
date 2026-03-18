import logging
import sys
from anomaly_analyzer import ProjectionAnomalyAnalyzer

def main():
    logging.basicConfig(
            level = logging.INFO,
            format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            # stream=sys.stdout
            datefmt = "%Y-%m-%d %H:%M:%S",
            filename= "logs/application.log",
            encoding= "utf-8"
        )
    logger = logging.getLogger(__name__)
    try:
        analyzer = ProjectionAnomalyAnalyzer()
        analyzer.start_analysis_loop()
    except Exception as e:
        logger.error(f"Ошибка запуска приложения: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())