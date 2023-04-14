package fr.gdd.sage.datasets;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class WDBenchTest {

    @Test
    public void creation_of_the_dataset_when_it_does_not_exist() {
        WDBench dataset = new WDBench(Optional.empty());
    }

}