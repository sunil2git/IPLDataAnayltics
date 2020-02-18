package pureScala

class empPojo {

  private[this]  var id : Int =_
  private[this]  var dept : Int =_
  private[this]  var name : String =_
  private[this]  var salary : Int =_



  def eid : Int = {
    id
  }
  def edept : Int = {
    dept
  }

  def ename : String = {
    name
  }
  def esalary : Int = {
    salary
  }

  // setters

  def id_= (i: Int) = {
    id = i
  }

  def dept_= (d: Int) = {
    dept = d
  }

  def name_= (n : String) = {
    name = n
  }

  def salary_= (s : Int) = {
    salary = s
  }

}
