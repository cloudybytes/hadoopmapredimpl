package com.cloudybytes.hadoopmapredimpl;

import com.cloudybytes.QueryGetter;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class HadoopmapredimplApplication  implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(HadoopmapredimplApplication.class, args);
	}

	@Override
	public void run(String... args){
		QueryGetter qg = new QueryGetter();
		System.out.println(qg.getParsedQuery("SELECT * FROM USERS INNER JOIN RATING on users.userid = rating.userid where age = 5 GROUP BY AGE HAVING AVG(RATING) >= 3"));
	}
}
