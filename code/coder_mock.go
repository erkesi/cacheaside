// Code generated by MockGen. DO NOT EDIT.
// Source: coder.go

// Package code is a generated GoMock package.
package code

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockCoder is a mock of Coder interface.
type MockCoder struct {
	ctrl     *gomock.Controller
	recorder *MockCoderMockRecorder
}

// MockCoderMockRecorder is the mock recorder for MockCoder.
type MockCoderMockRecorder struct {
	mock *MockCoder
}

// NewMockCoder creates a new mock instance.
func NewMockCoder(ctrl *gomock.Controller) *MockCoder {
	mock := &MockCoder{ctrl: ctrl}
	mock.recorder = &MockCoderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockCoder) EXPECT() *MockCoderMockRecorder {
	return m.recorder
}

// Decode mocks base method.
func (m *MockCoder) Decode(arg0 []byte, arg1 interface{}) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Decode", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Decode indicates an expected call of Decode.
func (mr *MockCoderMockRecorder) Decode(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Decode", reflect.TypeOf((*MockCoder)(nil).Decode), arg0, arg1)
}

// Encode mocks base method.
func (m *MockCoder) Encode(arg0 interface{}) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Encode", arg0)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Encode indicates an expected call of Encode.
func (mr *MockCoderMockRecorder) Encode(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Encode", reflect.TypeOf((*MockCoder)(nil).Encode), arg0)
}

// Name mocks base method.
func (m *MockCoder) Name() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Name")
	ret0, _ := ret[0].(string)
	return ret0
}

// Name indicates an expected call of Name.
func (mr *MockCoderMockRecorder) Name() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Name", reflect.TypeOf((*MockCoder)(nil).Name))
}
