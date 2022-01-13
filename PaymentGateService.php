<?php

declare(strict_types=1);

namespace App\PaymentGate\Service;

use App\Client\Interfaces\PaymentServiceInterface as ClientPaymentServiceInterface;
use App\Company\DBAL\Types\CompanyAlias;
use App\Company\Exception\Service\CompanyNotFoundException;
use App\Company\Interfaces\Service\CompanyServiceInterface;
use App\Loan\Interfaces\PaymentServiceInterface as LoanPaymentServiceInterface;
use App\PaymentGate\Builder\CompanyDTOBuilder;
use App\PaymentGate\Builder\MethodDTOBuilder;
use App\PaymentGate\Builder\TransactionDTOBuilder;
use App\PaymentGate\Builder\TransactionMetaDTOBuilder;
use App\PaymentGate\Constants\GetInfoServiceConstants;
use App\PaymentGate\Constants\Metric\PaymentGateMetricConstants;
use App\PaymentGate\DTO\BankCardDTO;
use App\PaymentGate\DTO\CompanyDTO;
use App\PaymentGate\DTO\Meta\DataParamsDTO;
use App\PaymentGate\DTO\Meta\UnbindBankCardMetaDTO;
use App\PaymentGate\DTO\Request\CardBindingRequest;
use App\PaymentGate\DTO\Request\ContextGetPaymentMethodsRequest;
use App\PaymentGate\DTO\Request\CreatePaymentRequest;
use App\PaymentGate\DTO\Request\GetCardsRequest;
use App\PaymentGate\DTO\Request\PaymentTransactionRequest;
use App\PaymentGate\DTO\Request\PayoutTransactionRequest;
use App\PaymentGate\DTO\Request\RecurrentPaymentTransactionRequest;
use App\PaymentGate\DTO\Request\ServiceGetPaymentMethodsRequest;
use App\PaymentGate\DTO\Response\CardBindingResponse;
use App\PaymentGate\DTO\Response\CreatePaymentResponse;
use App\PaymentGate\DTO\Response\GetCardsResponse;
use App\PaymentGate\DTO\Response\GetPaymentMethodResponse;
use App\PaymentGate\DTO\Response\PaymentTransactionDTO;
use App\PaymentGate\DTO\Response\TransactionDTO as TransactionDTOResponse;
use App\PaymentGate\DTO\Tinkoff\ConfigDTO;
use App\PaymentGate\DTO\TransactionDTO;
use App\PaymentGate\DTO\TransactionInfoDTO;
use App\PaymentGate\Entity\BankCard;
use App\PaymentGate\Entity\IntegrationConfig;
use App\PaymentGate\Entity\Method;
use App\PaymentGate\Entity\MethodCompany;
use App\PaymentGate\Entity\Transaction;
use App\PaymentGate\Event\Metric\PaymentMetricEvent;
use App\PaymentGate\Event\Metric\PayoutMetricEvent;
use App\PaymentGate\Exception\Adapter\ClientSuspiciousOperationException;
use App\PaymentGate\Exception\Adapter\DoTransactionErrorException;
use App\PaymentGate\Exception\Adapter\InvalidCardException;
use App\PaymentGate\Exception\Adapter\PaymentErrorException;
use App\PaymentGate\Exception\Adapter\PayoutErrorException;
use App\PaymentGate\Exception\Adapter\RecurrentPaymentErrorException;
use App\PaymentGate\Exception\BadRuleException;
use App\PaymentGate\Exception\BankCardNotFoundException;
use App\PaymentGate\Exception\BlockedPayoutByCardException;
use App\PaymentGate\Exception\Builder\TransactionMetaBuildException;
use App\PaymentGate\Exception\GeneralPaymentGateException;
use App\PaymentGate\Exception\PaymentGateContextException;
use App\PaymentGate\Exception\RecurrentTransactionNotFoundException;
use App\PaymentGate\Exception\Service\PaymentGateCompanyNotFoundException;
use App\PaymentGate\Exception\Service\PaymentGateInvalidServiceTypeException;
use App\PaymentGate\Exception\Service\PaymentGateMethodCompanyNotFoundException;
use App\PaymentGate\Exception\Service\PaymentGateMethodsNotFoundException;
use App\PaymentGate\Exception\Service\PaymentGateProviderNotFoundException;
use App\PaymentGate\Exception\TransactionNotFoundException;
use App\PaymentGate\Getter\PaymentGateGetter;
use App\PaymentGate\Getter\ProviderConfigGetter;
use App\PaymentGate\Interfaces\Client\ConfigDTOInterface;
use App\PaymentGate\Interfaces\PaymentGateServiceInterface;
use App\PaymentGate\Manager\BankCardManager;
use App\PaymentGate\Manager\CardCheckManager;
use App\PaymentGate\Manager\IntegrationConfigManager;
use App\PaymentGate\Manager\MethodCompanyManager;
use App\PaymentGate\Manager\MethodManager;
use App\PaymentGate\Manager\ReceiptManager;
use App\PaymentGate\Manager\TransactionManager;
use App\PaymentGate\Message\RecurrentPaymentMessage;
use App\PaymentGate\Repository\CompanyRepository;
use Psr\Log\LoggerInterface;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Stopwatch\Stopwatch;
use Throwable;

class PaymentGateService implements PaymentGateServiceInterface
{
    public function __construct(
        private MethodManager $methodManager,
        private CompanyServiceInterface $companyService,
        private ProviderConfigGetter $configGetter,
        private PaymentGateGetter $paymentGateGetter,
        private TransactionManager $transactionManager,
        private TransactionMetaDTOBuilder $transactionMetaDTOBuilder,
        private LoggerInterface $paymentGateLogger,
        private MethodCompanyManager $methodCompanyManager,
        private ReceiptManager $receiptManager,
        private MessageBusInterface $messageBus,
        private BankCardManager $bankCardManager,
        private EventDispatcherInterface $eventDispatcher,
        private TransactionDTOBuilder $transactionDTOBuilder,
        private IntegrationConfigManager $integrationConfigManager,
        private CompanyRepository $companyRepository,
        private LoanPaymentServiceInterface $loanPaymentService,
        private ClientPaymentServiceInterface $clientPaymentService,
        private CardCheckManager $cardCheckManager
    ) {
    }

    /**
     * {@inheritDoc}
     */
    public function getMethods(ServiceGetPaymentMethodsRequest $request, ?string $platform = null): GetPaymentMethodResponse
    {
        try {
            $company = $this->companyService->getCompanyByAlias($request->getCompanyAlias());
        } catch (CompanyNotFoundException $e) {
            throw PaymentGateCompanyNotFoundException::byAlias($request->getCompanyAlias());
        }

        $methods = $this->methodManager->getRepository()->getByCompanyAndDirection(
            $company->getAlias(),
            $request->getDirection()
        );

        if (!$methods) {
            throw PaymentGateMethodsNotFoundException::byAliasAndDirection($request->getCompanyAlias(), $request->getDirection());
        }

        return new GetPaymentMethodResponse(MethodDTOBuilder::byMethods($methods, $platform));
    }

    public function createPaymentTransaction(PaymentTransactionRequest $request): PaymentTransactionDTO
    {
        $stopwatch = (new Stopwatch())->start(PaymentGateMetricConstants::NAME_PAYMENT);

        $methodCompany = $this->methodCompanyManager->getRepository()->findMethodCompany(
            $request->getCompanyAlias(),
            $request->getPaymentMethodAlias(),
            $request->getPaymentProviderAlias(),
            Method::DIRECTION_INCOME
        );
        if (!$methodCompany) {
            throw new PaymentGateMethodCompanyNotFoundException();
        }

        $providerAlias     = $methodCompany->getMethod()->getProvider()->getAlias();
        $paymentGate       = $this->paymentGateGetter->byAlias($providerAlias);
        $integrationConfig = $this->getIntegrationConfig(
            [
                'action'  => 'payment',
                'company' => $request->getCompanyAlias(),
            ],
            $methodCompany
        );

        $receipt = null;
        if (!empty($request->getReceipt())) {
            $receipt = $this->receiptManager->createFromArray($request->getReceipt());
        }

        $transaction = $this->transactionManager->createTransactionByDirectionAndAmount(
            Transaction::TYPE_PAYMENT,
            $request->getAmount(),
            $integrationConfig,
            $methodCompany,
            $request->getClientId(),
            $request->getMeta()
        )->setReceipt($receipt);

        $transactionMeta = $this->transactionMetaDTOBuilder->buildPayment(
            $transaction,
            $request->getDataParams()
        );

        try {
            $paymentResponse = $paymentGate->doPayment($transaction, $transactionMeta);
        } catch (Throwable $exception) {
            $transaction->setStatus(Transaction::STATUS_FAILED);
            throw $exception;
        } finally {
            $this->transactionManager->create($transaction);

            $stopwatchEvent = $stopwatch->stop();

            $this->eventDispatcher->dispatch(
                new PaymentMetricEvent(
                    $providerAlias,
                    PaymentGateMetricConstants::TYPE_SINGLE_PAYMENT,
                    $transaction->getAmount(),
                    $transaction->getIntegrationConfig()->getConfig()[ConfigDTOInterface::FIELD_TERMINAL_KEY],
                    isset($exception) ? get_class($exception) : null,
                    (int) $stopwatchEvent->getDuration()
                )
            );
        }

        return $paymentResponse;
    }

    /**
     * @throws PaymentGateProviderNotFoundException
     * @throws PaymentGateMethodCompanyNotFoundException
     * @throws PayoutErrorException
     */
    public function createPayoutTransaction(PayoutTransactionRequest $request): TransactionDTOResponse
    {
        $bankCard = $this->bankCardManager->find($request->getCardId());

        if (null === $bankCard || $bankCard->getClientId() !== $request->getClientId()) {
            throw new PayoutErrorException('the card does not belong to the client');
        }

        $this->cardCheckManager->checkPayout($bankCard->getNumberMask(), $request->getCheckCardContext());

        $methodCompany = $this->methodCompanyManager->getRepository()->findMethodCompany(
            $request->getCompanyAlias(),
            $request->getPaymentMethodAlias(),
            $request->getPaymentProviderAlias(),
            Method::DIRECTION_OUTCOME
        );

        if (!$methodCompany) {
            throw new PaymentGateMethodCompanyNotFoundException();
        }

        $config = $this->getIntegrationConfig(
            [
                'action'  => 'payout',
                'company' => CompanyAlias::DZP_RAZVITIYE,
            ],
            $methodCompany
        );

        $transaction = $this->transactionManager->createTransactionByDirectionAndAmount(
            Transaction::TYPE_PAYOUT,
            $request->getAmount(),
            $config,
            $methodCompany,
            $request->getClientId(),
            $request->getMeta()
        )->setBankCard($bankCard);

        $this->transactionManager->create($transaction);

        return new TransactionDTOResponse($transaction->getId());
    }

    public function bindCard(CardBindingRequest $request): CardBindingResponse
    {
        try {
            $paymentGate       = $this->paymentGateGetter->byAlias(PaymentGateGetter::PROVIDER_TINKOFF);
            $integrationConfig = $this->getIntegrationConfig(
                [
                    'action'  => 'bindCard',
                    'company' => CompanyAlias::DZP_RAZVITIYE,
                ]
            );

            $cardBindingUrl = $paymentGate->getBindBankCardUrl(
                $request->getClientId(),
                $integrationConfig,
                $request->getEmail(),
                $request->getPhone()
            );

            return new CardBindingResponse(CardBindingResponse::TYPE_FRAME, ['url' => $cardBindingUrl]);
        } catch (Throwable $exception) {
            throw new GeneralPaymentGateException($exception->getMessage(), 0, $exception);
        }
    }

    public function getCards(GetCardsRequest $request): GetCardsResponse
    {
        $cards = $this->bankCardManager->getByClientIdAndType($request->getClientId(), $request->getTypeCard());

        /*
         * Filter for unique cards
         */
        if ($request::CARD_TYPE_RECURRENT === $request->getTypeCard()) {
            $uniqueCards = [];
            foreach ($cards as $card) {
                if (array_key_exists($card->getNumberMask(), $uniqueCards)) {
                    continue;
                }
                $uniqueCards[$card->getNumberMask()] = $card;
            }
            $cards = $uniqueCards;
        }

        if ($request::CARD_TYPE_PAYOUT === $request->getTypeCard() && null !== $request->getCheckCardContext()) {
            $cards = array_filter(
                $cards,
                function (BankCard $card) use ($request): bool {
                    try {
                        $this->cardCheckManager->checkPayout($card->getNumberMask(), $request->getCheckCardContext());
                    } catch (BlockedPayoutByCardException) {
                        return false;
                    }

                    return true;
                }
            );
        }

        $cardsDTO = array_map(
            static function (BankCard $card): BankCardDTO {
                return new BankCardDTO(
                    $card->getId(),
                    (int) $card->getExpireDate()->format('y'),
                    (int) $card->getExpireDate()->format('m'),
                    $card->getNumberMask(),
                    (bool) $card->getIsRecurrent()
                );
            },
            array_values($cards)
        );

        return new GetCardsResponse($cardsDTO);
    }

    /**
     * @throws TransactionNotFoundException
     */
    public function getTransactionById(int $id): TransactionDTO
    {
        $tx = $this->transactionManager->getTransactionById($id);

        return $this->transactionDTOBuilder->buildDTO($tx);
    }

    /**
     * @throws TransactionNotFoundException
     */
    public function getTransactionInfoById(int $id): TransactionInfoDTO
    {
        $tx = $this->transactionManager->getTransactionById($id);

        return $this->transactionDTOBuilder->buildInfoDTO($tx);
    }

    /**
     * @param array<string,mixed> $searchInfo
     *
     * @throws PaymentGateProviderNotFoundException
     */
    private function getIntegrationConfig(array $searchInfo, ?MethodCompany $methodCompany = null): IntegrationConfig
    {
        try {
            return $this->configGetter->getConfig($searchInfo, $methodCompany);
        } catch (BadRuleException $e) {
            $this->paymentGateLogger->error(
                'Corrupted matching rule',
                [
                    'searched-fields' => $searchInfo,
                    'error'           => $e->getMessage(),
                ]
            );
            throw new PaymentGateProviderNotFoundException();
        } catch (PaymentGateProviderNotFoundException $e) {
            $this->paymentGateLogger->error(
                'Integration not found',
                [
                    'searched-fields' => $searchInfo,
                ]
            );
            throw $e;
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GeneralPaymentGateException
     */
    public function removeBankCard(int $cardId): void
    {
        if (!$card = $this->bankCardManager->find($cardId)) {
            throw new BankCardNotFoundException();
        }

        if (BankCard::TYPE_PAYOUT !== $card->getType()) {
            throw new BankCardNotFoundException();
        }

        try {
            $paymentGate = $this->paymentGateGetter->byAlias(PaymentGateGetter::PROVIDER_TINKOFF);

            $meta = null;

            if ($bindCard = $card->getBindCard()) {
                $meta = new UnbindBankCardMetaDTO($bindCard->getIntegrationConfig());
            } else {
                $config = $this->integrationConfigManager->getByConfigType(ConfigDTO::TYPE_E2C);

                if (null !== $config) {
                    $meta = new UnbindBankCardMetaDTO($config);
                }
            }

            if (null !== $meta) {
                $paymentGate->getUnbindBankCardUrl($card, $meta);
            }

            $this->bankCardManager->remove($card);
        } catch (InvalidCardException) {
            $this->bankCardManager->remove($card);
        } catch (Throwable $exception) {
            $this->paymentGateLogger->critical(
                __METHOD__,
                [
                    'exception' => [
                        'name'    => get_class($exception),
                        'message' => $exception->getMessage(),
                        'trace'   => $exception->getTraceAsString(),
                    ],
                ]
            );

            throw new GeneralPaymentGateException($exception->getMessage(), 0, $exception);
        }
    }

    /**
     * @throws CompanyNotFoundException
     */
    public function getCompanyByAlias(string $alias): CompanyDTO
    {
        $company = $this->companyRepository->findOneBy(['alias' => $alias]);
        if (!$company) {
            throw CompanyNotFoundException::byAlias($alias);
        }

        return CompanyDTOBuilder::buildFromEntity($company);
    }

    public function createPayment(CreatePaymentRequest $request): CreatePaymentResponse
    {
        return match ($request->getServiceType()) {
            GetInfoServiceConstants::SERVICE_LOAN   => $this->loanPaymentService->createPayment($request),
            GetInfoServiceConstants::SERVICE_OPTION => $this->clientPaymentService->createOptionPayment(
                $request
            ),
            default => throw PaymentGateInvalidServiceTypeException::create($request->getServiceType()),
        };
    }

    public function getContextMethods(ContextGetPaymentMethodsRequest $contextRequest): GetPaymentMethodResponse
    {
        $serviceRequest =  match ($contextRequest->getServiceType()) {
            GetInfoServiceConstants::SERVICE_LOAN => $this->loanPaymentService->getPaymentMethodsRequest(
                $contextRequest
            ),
            GetInfoServiceConstants::SERVICE_OPTION => $this->clientPaymentService->getPaymentMethodsRequest(
                $contextRequest
            ),
            default => throw PaymentGateInvalidServiceTypeException::create($contextRequest->getServiceType()),
        };

        return $this->getMethods($serviceRequest, $contextRequest->getPlatform());
    }
}
