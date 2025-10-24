package com.example.person_service.repository;

import com.example.person_service.entity.Person;
import java.time.LocalDate;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Page;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface PersonRepository extends JpaRepository<Person, UUID>, JpaSpecificationExecutor<Person>, PagingAndSortingRepository<Person, UUID> {
    Optional<Person> findByTaxNumber(String taxNumber);
    Optional<Person> deleteByTaxNumber(String taxNumber);
    Page<Person> findAll(Pageable pageable);

    @Query("SELECT p FROM Person p WHERE " +
            "(LOWER(p.firstName) LIKE LOWER(CONCAT(:prefix, '%')) OR " +
            "LOWER(p.lastName) LIKE LOWER(CONCAT(:prefix, '%'))) AND " +
            "p.dob <= :maxDob")
    Page<Person> findByNamePrefixAndMinAge(
            @Param("prefix") String prefix,
            @Param("maxDob") LocalDate maxDob,
            Pageable pageable
    );
    default Page<Person> findByNamePrefixAndMinAge(
            String prefix,
            int minAge,
            Pageable pageable
    ) {
        LocalDate maxDob = LocalDate.now().minusYears(minAge);
        return findByNamePrefixAndMinAge(prefix, maxDob, pageable);
    }
    Page<Person> findByFirstNameStartingWithIgnoreCaseOrLastNameStartingWithIgnoreCase(
            String firstNamePrefix,
            String lastNamePrefix,
            Pageable pageable
    );

    Page<Person> findPersonByDobAfter(LocalDate dob, Pageable pageable);

    void deletePersonById(@Param("id") UUID id);
}