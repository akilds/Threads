package FileHandling;

import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class EmployeePayrollServiceTest {
	@Test
	public void employeePayrollCheck()
	{
		EmployeePayrollData[] arrayOfEmps = {
				new EmployeePayrollData(1,"Jeff Bezos",10000.0),
				new EmployeePayrollData(2,"Bill Gates",20000.0),
				new EmployeePayrollData(3,"Mark Zuckerburg",30000.0)
		};
		EmployeePayrollService employeePayrollService;
		employeePayrollService = new EmployeePayrollService(Arrays.asList(arrayOfEmps));
		employeePayrollService.writeEmployeePayrollData(EmployeePayrollService.IOService.FILE_IO);
		employeePayrollService.printData(EmployeePayrollService.IOService.FILE_IO);
		long entries = employeePayrollService.countEntries(EmployeePayrollService.IOService.FILE_IO);
		Assert.assertEquals(3,entries);
	}
	
	@Test
	public void employeePayrollCheck2()
	{
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		long entries = employeePayrollService.readEmployeePayrollData(EmployeePayrollService.IOService.FILE_IO);
		Assert.assertEquals(3,entries);
	}
	
	@Test
	public void matchCount()
	{
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readEmployeePayrollData1(EmployeePayrollService.IOService.DB_IO);
		Assert.assertEquals(3, employeePayrollData.size());
	}
	
	@Test
	public void testUpdation()
	{
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readEmployeePayrollData1(EmployeePayrollService.IOService.DB_IO);
		employeePayrollService.updateEmployeeSalary("Terisa",3000000.00);
		boolean result = employeePayrollService.checkEmployeePayrollInSyncWithDB("Terisa");
		Assert.assertTrue(result);
	}
	
	@Test
	public void testRetrieval()
	{
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		employeePayrollService.readEmployeePayrollData1(EmployeePayrollService.IOService.DB_IO);
		LocalDate startDate = LocalDate.of(2018, 01, 01);
		LocalDate endDate = LocalDate.now();
		List<EmployeePayrollData> employeePayrollData = 
				employeePayrollService.readEmployeePayrollForDateRange(EmployeePayrollService.IOService.DB_IO, startDate, endDate);
		Assert.assertEquals(3, employeePayrollData.size());
	}
	
	@Test
	public void testRetrieval2()
	{
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		employeePayrollService.readEmployeePayrollData1(EmployeePayrollService.IOService.DB_IO);
		Map<String, Double> averageSalaryByGender = employeePayrollService.readAverageSalaryByGender(EmployeePayrollService.IOService.DB_IO);
		Assert.assertTrue(averageSalaryByGender.get("M").equals(2000000.00) &&
				          averageSalaryByGender.get("F").equals(3000000.00) );
	}
	
	public void testAdd()
	{
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		employeePayrollService.readEmployeePayrollData1(EmployeePayrollService.IOService.DB_IO);
		employeePayrollService.addEmployeeToPayroll("Mark",5000000.00,LocalDate.now(),"M");
		boolean result = employeePayrollService.checkEmployeePayrollInSyncWithDB("Mark");
		Assert.assertTrue(result);
	}
	
	public void testMultiThread()
	{
		EmployeePayrollData[] arrayOfEmps = {
				new EmployeePayrollData(0,"Jeff Bezos","M",100000.0,LocalDate.now()),
				new EmployeePayrollData(0,"Bill Gates","M",200000.0,LocalDate.now()),
				new EmployeePayrollData(0,"Mark Zuckerburg","M",300000.0,LocalDate.now()),
				new EmployeePayrollData(0,"Sunder","M",600000.0,LocalDate.now()),
				new EmployeePayrollData(0,"Mukesh","M",1000000.0,LocalDate.now()),
				new EmployeePayrollData(0,"Anil","M",200000.0,LocalDate.now())
		};
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		employeePayrollService.readEmployeePayrollData(EmployeePayrollService.IOService.DB_IO);
		Instant start = Instant.now();
		employeePayrollService.addEmployeesToPayroll(Arrays.asList(arrayOfEmps));
		Instant end = Instant.now();
		System.out.println("Duration without Thread : " + Duration.between(start, end));
		Instant threadStart = Instant.now();
		employeePayrollService.addEmployeesToPayrollWithThreads(Arrays.asList(arrayOfEmps));
		Instant threadEnd = Instant.now();
		System.out.println("Duration with Thread : " + Duration.between(threadStart, threadEnd));
		Assert.assertEquals(13, employeePayrollService.countEntries(EmployeePayrollService.IOService.DB_IO));
	}
}
