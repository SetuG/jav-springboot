package com.example.sqlsolution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

@SpringBootApplication
public class SqlSolutionApp implements CommandLineRunner {

    private static final Logger LOGGER = Logger.getLogger(SqlSolutionApp.class.getName());

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Environment env;

    private static final String GENERATE_WEBHOOK_URL = "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA";

    public SqlSolutionApp(Environment env) {
        this.env = env;
        restTemplate.setRequestFactory(new org.springframework.http.client.SimpleClientHttpRequestFactory());
    }

    public static void main(String[] args) {
        SpringApplication.run(SqlSolutionApp.class, args);
    }

    @Override
    public void run(String... args) {
        LOGGER.info("Starting submission flow...");

        String name = getConfig("name", "John Doe");
        String regNo = getConfig("regNo", "REG12347");
        String email = getConfig("email", "john@example.com");

        try {

            Map<String, Object> webhookResponse = generateWebhook(name, regNo, email);
            if (webhookResponse == null) {
                LOGGER.severe("Failed to generate webhook. Aborting.");
                return;
            }

            String webhookUrl = (String) webhookResponse.get("webhook");
            String accessToken = (String) webhookResponse.get("accessToken");

            if (webhookUrl == null || accessToken == null) {
                LOGGER.severe("Webhook URL or access token missing in response. Aborting.");
                return;
            }

            LOGGER.info("Received webhook URL and access token.");


            String finalQuery = buildFinalQuery();
            saveQueryToFile(finalQuery, "final-query.sql");
            boolean submitted = submitFinalQuery(webhookUrl, accessToken, finalQuery);
            if (submited) {
                LOGGER.info("Submission complete.");
            } else {
                LOGGER.warning("Submission failed.");
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unexpected error during run: " + e.getMessage(), e);
        }
    }

    private String getConfig(String key, String defaultValue) {
        String fromEnv = System.getenv(key.replace('.', '_').toUpperCase());
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv;
        }
        String fromProps = env.getProperty(key);
        return fromProps != null ? fromProps : defaultValue;
    }

    private Map<String, Object> generateWebhook(String name, String regNo, String email) {
        try {
            LOGGER.info("Calling generateWebhook API...");

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            Map<String, String> body = new HashMap<>();
            body.put("name", name);
            body.put("regNo", regNo);
            body.put("email", email);

            HttpEntity<Map<String, String>> request = new HttpEntity<>(body, headers);

            ResponseEntity<String> response = restTemplate.postForEntity(GENERATE_WEBHOOK_URL, request, String.class);

            if (response.getStatusCode() != HttpStatus.OK && response.getStatusCode() != HttpStatus.CREATED) {
                LOGGER.severe("generateWebhook returned non-OK status: " + response.getStatusCode());
                return null;
            }

            JsonNode node = objectMapper.readTree(Objects.requireNonNull(response.getBody()));
            String webhook = node.has("webhook") ? node.get("webhook").asText() : null;
            String accessToken = node.has("accessToken") ? node.get("accessToken").asText() : null;

            Map<String, Object> result = new HashMap<>();
            result.put("webhook", webhook);
            result.put("accessToken", accessToken);
            return result;

        } catch (HttpClientErrorException e) {
            LOGGER.log(Level.SEVERE, "HTTP error calling generateWebhook: " + e.getStatusCode() + " - " + e.getResponseBodyAsString(), e);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error while generating webhook: " + e.getMessage(), e);
        }
        return null;
    }

    private boolean submitFinalQuery(String webhookUrl, String accessToken, String finalQuery) {
        try {
            LOGGER.info("Submitting final SQL query to webhook: " + webhookUrl);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBearerAuth(accessToken);

            Map<String, String> body = new HashMap<>();
            body.put("finalQuery", finalQuery);

            HttpEntity<Map<String, String>> request = new HttpEntity<>(body, headers);

            ResponseEntity<String> response = restTemplate.postForEntity(webhookUrl, request, String.class);

            LOGGER.info("Webhook response status: " + response.getStatusCode());
            LOGGER.info("Webhook response body: " + response.getBody());

            return response.getStatusCode().is2xxSuccessful();

        } catch (HttpClientErrorException e) {
            LOGGER.log(Level.SEVERE, "HTTP error submitting final query: " + e.getStatusCode() + " - " + e.getResponseBodyAsString(), e);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error while submitting final query: " + e.getMessage(), e);
        }
        return false;
    }

    private String buildFinalQuery() {
        return "SELECT p.AMOUNT AS SALARY, " +
                "CONCAT(e.FIRST_NAME, ' ', e.LAST_NAME) AS NAME, " +
                "TIMESTAMPDIFF(YEAR, e.DOB, CURDATE()) AS AGE, " +
                "d.DEPARTMENT_NAME " +
                "FROM PAYMENTS p " +
                "JOIN EMPLOYEE e ON p.EMP_ID = e.EMP_ID " +
                "JOIN DEPARTMENT d ON e.DEPARTMENT = d.DEPARTMENT_ID " +
                "WHERE DAY(p.PAYMENT_TIME) <> 1 " +
                "ORDER BY p.AMOUNT DESC " +
                "LIMIT 1;";
    }

    private void saveQueryToFile(String sql, String filename) {
        try (FileWriter writer = new FileWriter(filename, false)) {
            writer.write(sql);
            LOGGER.info("Saved final query to " + filename);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Could not write final query to file: " + e.getMessage(), e);
        }
    }
}
