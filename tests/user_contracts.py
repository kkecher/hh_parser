#!/usr/bin/env python3

from contracts import contract, new_contract

module_contract = new_contract("module_contract", lambda m: isinstance(m, module))
# def digit_form_contract_test_cases():
#     digit_form_contract.check('0:5:3')
#     digit_form_contract.check('00:5:3')
#     digit_form_contract.check('0:15:3')
#     digit_form_contract.check('00:15:3')
#     digit_form_contract.check('0:5:03')
#     digit_form_contract.check('0:5:32')
#     digit_form_contract.check('0:3')
#     digit_form_contract.check('  0    : 3           ')
#     digit_form_contract.check('0:03')
#     digit_form_contract.check('0:12')
#     digit_form_contract.check('00:12')
#     digit_form_contract.check('5:0')
#     digit_form_contract.check('05:00')
#     digit_form_contract.check('180:05')
#     digit_form_contract.check('00:73:300')
#     digit_form_contract.check('0:0:0')
#     digit_form_contract.check('2')
#     digit_form_contract.check('2:')
#     digit_form_contract.check(':20')
#     digit_form_contract.check('-20')
#     digit_form_contract.check(':15:4')
#     digit_form_contract.check('4:2')
#     digit_form_contract.check(':2')
#     digit_form_contract.check('05:4:')
#     digit_form_contract.check('100:04:')
#     digit_form_contract.check(':444:')
#     digit_form_contract.check('0')
#     digit_form_contract.check('4:44:443:333')
#     digit_form_contract.fail('foo')
#     digit_form_contract.fail('bar:foo:baz')
#     digit_form_contract.fail(0)
#     digit_form_contract.fail(100)
#     digit_form_contract.fail(-20)
# digit_form_contract_test_cases()

# @contract(user_time=digit_form_contract, returns='int,>=0')
# def transform_digit_form_to_seconds(user_time):
#     '''
#     Transform time in format hh?:mm?:ss? to seconds
#     '''
