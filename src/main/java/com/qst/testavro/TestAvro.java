package com.qst.testavro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import Tutorialspoint.Employee;

public class TestAvro {

	/**
	 * 测试avro的序列化
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSerial() throws Exception {

		Employee e = new Employee();
		e.setAge(10);
		e.setName("唐洁");

		//
		DatumWriter<Employee> dw = new SpecificDatumWriter<Employee>(Employee.class);

		// 开始文件串行化
		DataFileWriter<Employee> dfw = new DataFileWriter<Employee>(dw);
		// 将串行化文件实例化到磁盘
		dfw.create(e.getSchema(), new File("D:/avro/emp.avro"));
		// 在串行化数据中，将对象写入
		dfw.append(e);

		dfw.close();
		System.out.println("ok");

	}

	/**
	 * 测试avro的反序列化
	 * 
	 * @throws Exception
	 */
	@Test
	public void testDeserial() throws Exception {

		Employee e = new Employee();

		DatumReader<Employee> dr = new SpecificDatumReader<Employee>(Employee.class);

		DataFileReader<Employee> dfr = new DataFileReader<Employee>(new File("D:/avro/emp.avro"), dr);

		while (dfr.hasNext()) {
			System.out.println(dfr.next(e));
		}

	}

	/**
	 * 不通过生成类，直接通过schema对对象进行串行
	 * 
	 * @throws Exception
	 */
	@Test
	public void testParserSerial() throws Exception {

		Schema schema = new Schema.Parser().parse(new File("D:/avro/emp.avsc"));

		// 将schema初始化成对象
		GenericRecord e = new GenericData.Record(schema);

		// 在schema对象中，添加数据
		e.put("age", 10);
		e.put("Name", "tom");

		//
		DatumWriter<GenericRecord> dw = new SpecificDatumWriter<GenericRecord>(schema);

		// 开始文件串行化
		DataFileWriter<GenericRecord> dfw = new DataFileWriter<GenericRecord>(dw);
		// 将串行化文件实例化到磁盘
		dfw.create(schema, new File("D:/avro/emp2.avro"));

		dfw.append(e);
		dfw.close();
		System.out.println("ok");

	}

	/**
	 * 通过parser进行反序列化
	 * 
	 * @throws Exception
	 */
	@Test
	public void testParserDeserial() throws Exception {

		Schema schema = new Schema.Parser().parse(new File("D:/avro/emp.avsc"));

		DatumReader<GenericRecord> dr = new SpecificDatumReader<GenericRecord>(schema);

		DataFileReader<GenericRecord> dfr = new DataFileReader<GenericRecord>(new File("D:/avro/emp2.avro"), dr);

		GenericRecord emp = null;
		while (dfr.hasNext()) {
			
			emp = dfr.next(emp);
			
			System.out.println(emp);
		}
	}

}
