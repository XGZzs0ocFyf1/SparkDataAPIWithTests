import java.time.{LocalDateTime, LocalTime}
import java.time.format.DateTimeFormatter


object Helpers {

  //default dtf 2018-01-25 02:10:58|
  implicit class StringToLdt(ldt: String) {
    def toLdt() = {
      val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      LocalDateTime.parse(ldt, dtf)
    }
  }


  implicit class TimeConverter(ldt: LocalDateTime) {
    //utils
    //split time to 1/4 of hour to get more representative data
    def changeTimeToQuoters: LocalTime = {
      ldt.getMinute match {
        case q if 0 until 15 contains q => LocalTime.of(ldt.getHour, 0)
        case q if 15 until 30 contains q => LocalTime.of(ldt.getHour, 15)
        case q if 30 until 45 contains q => LocalTime.of(ldt.getHour, 30)
        case q if 45 until 60 contains q => LocalTime.of(ldt.getHour, 45)
      }
    }
  }


}
