package org.example.kafkademo.controllers;

import org.example.kafkademo.dto.CreateProductDto;
import org.example.kafkademo.service.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/products") // http://localhost:<Tomcat port>/products
public class ProductController {
    private final ProductService productService;
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    @PostMapping
    public ResponseEntity<String> createProduct(
            @RequestBody CreateProductDto product) {
        String productId = productService.createProduct(product);
        return ResponseEntity.status(HttpStatus.CREATED).body(productId);
    }

    /*
    SYNC WAY controller

    @PostMapping
    public ResponseEntity<Object> createProduct(
            @RequestBody CreateProductDto product) throws ExecutionException,
             InterruptedException {
        String productId;

        try {
        productId = productService.createProduct(product);
        } catch(Exception e) {
        LOGGER.error("Error from createProduct controller" + e.getMessage(), e);

        // create a class ErrorMessage to handle errors
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
        .body(new ErrorMessage(new Date(), e.getMessage(), "/products");
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(productId);
    }

     */

}
