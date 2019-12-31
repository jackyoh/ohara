/*
 * Copyright 2019 is-land
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import styled, { css } from 'styled-components';
import { NavLink } from 'react-router-dom';

export const Header = styled.header(
  ({ theme }) => css`
    display: flex;
    flex-direction: column;
    align-items: center;
    width: 70px;
    min-width: 70px;
    background: ${theme.palette.primary[900]};

    .brand {
      margin-top: ${theme.spacing(2)}px;
      margin-bottom: ${theme.spacing(3)}px;
    }
  `,
);

export const WorkspaceList = styled.ul(
  ({ theme }) => css`
    display: flex;
    align-items: center;
    flex-direction: column;

    .current {
      .item {
        color: ${theme.palette.grey[400]};
        background-color: ${theme.palette.grey[300]};
      }
    }

    .item {
      margin-bottom: ${theme.spacing(2)}px;
      border-radius: ${theme.shape.borderRadius}px;
      background-color: ${theme.palette.grey[100]};
      color: ${theme.palette.grey[500]};
      font-size: 20px;
      width: 40px;
      height: 40px;
      display: flex;
      justify-content: center;
      align-items: center;
      cursor: pointer;
    }

    .add-workspace {
      background-color: transparent;
      border: 1px solid ${theme.palette.common.white};
      color: ${theme.palette.common.white};
      display: flex;
      align-items: center;
      justify-content: center;
      transition: ${theme.transitions.create('background-color')};

      &:hover {
        background-color: ${theme.palette.common.white};
        svg {
          fill: ${theme.palette.primary[900]};
        }
      }
    }
  `,
);

export const Tools = styled.ul`
  /* This moves tools to the bottom of the bar */
  margin-top: auto;
  display: flex;
  flex-direction: column;
  font-size: 20px;
  align-items: center;
  width: 100%;
  color: ${props => props.theme.palette.common.white};

  .item {
    margin-bottom: ${props => props.theme.spacing(2)}px;
    cursor: pointer;

    &:hover {
      opacity: 0.9;
    }
  }
`;

export const StyledNavLink = styled(NavLink)(
  props => css`
    text-decoration: none;
    color: ${props.theme.palette.grey[400]};

    &.active-link {
      border: 2px solid ${props.theme.palette.primary[400]};
    }
  `,
);