package org.example.kafkademo.service.interfaces;

import org.example.kafkademo.dto.CreateProductDto;

import java.util.concurrent.ExecutionException;

public interface ProductServiceInterface {
    String createProduct(CreateProductDto dto);

    /* SYNC WAY Interface
    String createProduct(CreateProductDto dto) throws ExecutionException, InterruptedException;
    */
}
