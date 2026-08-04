package com.vishal.payflo.validators;

import com.vishal.payflo.advice.enums.ErrorCode;
import com.vishal.payflo.advice.exceptions.InvalidVpaException;
import com.vishal.payflo.configs.ExceptionMessagesProperties;
import com.vishal.payflo.configs.VpaPaymentServiceProviderProperties;
import com.vishal.payflo.dtos.paymentdetails.UpiDetails;
import com.vishal.payflo.enums.PaymentType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

@Component
@Slf4j
public class UpiValidator implements PaymentValidator<UpiDetails> {

    private static final Pattern VPA_IDENTIFIER_PATTERN = Pattern.compile("^[a-zA-Z0-9._-]{3,60}$");
    private static final Pattern VPA_PAYMENT_SERVICE_PROVIDER_PATTERN = Pattern.compile("^[a-z]{3,39}$");


    private final Set<String> paymentServiceProvidersSet;
    private final ExceptionMessagesProperties exceptionMessages;

    public UpiValidator(VpaPaymentServiceProviderProperties paymentServiceProviderProperties,
                        ExceptionMessagesProperties exceptionMessages) {
        this.paymentServiceProvidersSet = new HashSet<>(paymentServiceProviderProperties.paymentServiceProviders());
        this.exceptionMessages = exceptionMessages;
    }

    @Override
    public void validate(UpiDetails paymentDetails) {
        String vpa = paymentDetails.vpa();

        String[] parts = vpa.split("@", -1);

        if (parts.length != 2) {
            logValidationRejectedWarning(ErrorCode.VPA_SEPARATOR_COUNT_MISMATCH);
            throw new InvalidVpaException(exceptionMessages.vpaSeparatorCountMismatch(), ErrorCode.VPA_SEPARATOR_COUNT_MISMATCH);
        }

        String identifier = parts[0];
        String psp = parts[1];


        if (!VPA_IDENTIFIER_PATTERN.matcher(identifier).matches()) {
            logValidationRejectedWarning(ErrorCode.VPA_INVALID_IDENTIFIER_FORMAT);
            throw new InvalidVpaException(exceptionMessages.vpaInvalidIdentifierFormat(), ErrorCode.VPA_INVALID_IDENTIFIER_FORMAT);
        }

        if (!VPA_PAYMENT_SERVICE_PROVIDER_PATTERN.matcher(psp).matches()) {
            logValidationRejectedWarning(ErrorCode.VPA_INVALID_PAYMENT_SERVICE_PROVIDER_FORMAT);
            throw new InvalidVpaException(exceptionMessages.vpaInvalidPaymentServiceProviderFormat(), ErrorCode.VPA_INVALID_PAYMENT_SERVICE_PROVIDER_FORMAT);
        }

        if (!paymentServiceProvidersSet.contains(psp)) {
            logValidationRejectedWarning(ErrorCode.VPA_UNKNOWN_PAYMENT_SERVICE_PROVIDER);
            throw new InvalidVpaException(exceptionMessages.vpaUnknownPaymentServiceProvider(), ErrorCode.VPA_UNKNOWN_PAYMENT_SERVICE_PROVIDER);
        }
    }

    private void logValidationRejectedWarning(ErrorCode errorCode) {
        log.warn("Validation rejected for payment details type:{} errorCode:{}",PaymentType.UPI, errorCode);
    }
}
